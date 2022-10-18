//! Optimizer rule eliminating/moving Aggregate Expr(s) with a `DISTINCT` inner Expr.
//!
//! Dask-SQL performs a `DISTINCT` operation with its Aggregation code base. That Aggregation
//! is expected to have a specific format however. It should be an Aggregation LogicalPlan
//! variant that has `group_expr` = "column you want to distinct" with NO `agg_expr` present
//!
//! A query like
//! ```text
//! SELECT
//!     COUNT(DISTINCT a)
//! FROM test
//! ```
//!
//! Would typically produce a LogicalPlan like ...
//! ```text
//! Projection: COUNT(a.a) AS COUNT(DISTINCT a.a)))\
//!   Aggregate: groupBy=[[]], aggr=[[COUNT(a.a)]]\
//!     Aggregate: groupBy=[[a.a]], aggr=[[]]\
//!       TableScan: test";
//! ```
//!
//! If the query has both a COUNT and a COUNT DISTINCT on the same expression then we need to
//! first perform an aggregate with a group by and the COUNT(*) for each grouping key, then
//! we need to COUNT the number of rows to produce the COUNT DISTINCT result and SUM the values
//! of the COUNT(*) values to get the COUNT value.
//!
//! For example:
//!
//! ```text
//! SELECT
//!     COUNT(a), COUNT(DISTINCT a)
//! FROM test
//! ```
//!
//! Would typically produce a LogicalPlan like ...
//! ```text
//! Projection: SUM(alias2) AS COUNT(a), COUNT(alias1) AS COUNT(DISTINCT a)
//!   Aggregate: groupBy=[[]], aggr=[[SUM(alias2), COUNT(alias1)]]
//!     Aggregate: groupBy=[[a AS alias1]], aggr=[[COUNT(*) AS alias2]]
//!       TableScan: test projection=[a]
//!
//! If the query contains DISTINCT aggregates for multiple columns then we need to perform
//! separate aggregate queries per column and then join the results. The final Dask plan
//! should like like this:
//!
//! CrossJoin:\
//!  CrossJoin:\
//!    CrossJoin:\
//!      Projection: SUM(__dask_sql_count__1) AS COUNT(a.a), COUNT(a.a) AS COUNT(DISTINCT a.a)\
//!        Aggregate: groupBy=[[]], aggr=[[SUM(__dask_sql_count__1), COUNT(a.a)]]\
//!          Aggregate: groupBy=[[a.a]], aggr=[[COUNT(UInt64(1)) AS __dask_sql_count__1]]\
//!            TableScan: a\
//!      Projection: SUM(__dask_sql_count__2) AS COUNT(a.b), COUNT(a.b) AS COUNT(DISTINCT(a.b))\
//!        Aggregate: groupBy=[[]], aggr=[[SUM(__dask_sql_count__2), COUNT(a.b)]]\
//!          Aggregate: groupBy=[[a.b]], aggr=[[COUNT(UInt64(1)) AS __dask_sql_count__2]]\
//!            TableScan: a\
//!    Projection: SUM(__dask_sql_count__3) AS COUNT(a.c), COUNT(a.c) AS COUNT(DISTINCT(a.c))\
//!      Aggregate: groupBy=[[]], aggr=[[SUM(__dask_sql_count__3), COUNT(a.c)]]\
//!        Aggregate: groupBy=[[a.c]], aggr=[[COUNT(UInt64(1)) AS __dask_sql_count__3]]\
//!          TableScan: a\
//!  Projection: SUM(__dask_sql_count__4) AS COUNT(a.d), COUNT(a.d) AS COUNT(DISTINCT(a.d))\
//!    Aggregate: groupBy=[[]], aggr=[[SUM(__dask_sql_count__4), COUNT(a.d)]]\
//!      Aggregate: groupBy=[[a.d]], aggr=[[COUNT(UInt64(1)) AS __dask_sql_count__4]]\
//!        TableScan: a

use std::{collections::HashSet, sync::Arc};

use datafusion_common::{Column, Result};
use datafusion_expr::{
    col,
    count,
    logical_plan::{Aggregate, LogicalPlan, Projection},
    AggregateFunction,
    Expr,
    LogicalPlanBuilder,
};
use datafusion_optimizer::{utils, OptimizerConfig, OptimizerRule};
use log::trace;

/// Optimizer rule eliminating/moving Aggregate Expr(s) with a `DISTINCT` inner Expr.
#[derive(Default)]
pub struct EliminateAggDistinct {}

impl EliminateAggDistinct {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateAggDistinct {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        // optimize inputs first
        let plan = utils::optimize_children(self, plan, optimizer_config)?;

        match plan {
            LogicalPlan::Aggregate(Aggregate {
                ref input,
                ref group_expr,
                ref aggr_expr,
                ..
            }) => {
                // first pass collects the expressions being aggregated
                let mut distinct_columns: HashSet<Expr> = HashSet::new();
                let mut not_distinct_columns: HashSet<Expr> = HashSet::new();
                for expr in aggr_expr {
                    gather_expressions(expr, &mut distinct_columns, &mut not_distinct_columns);
                }

                // combine the two sets to get all unique expressions
                let mut unique_expressions = distinct_columns.clone();
                unique_expressions.extend(not_distinct_columns.clone());

                let unique_expressions = unique_set_without_aliases(&unique_expressions);

                let mut x: Vec<Expr> = Vec::from_iter(unique_expressions);
                x.sort_by(|l, r| format!("{}", l).cmp(&format!("{}", r)));

                let plans: Vec<LogicalPlan> = x
                    .iter()
                    .map(|expr| {
                        create_plan(
                            &plan,
                            input,
                            expr,
                            group_expr,
                            &distinct_columns,
                            &not_distinct_columns,
                            optimizer_config,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

                for plan in &plans {
                    trace!("{}", plan.display_indent());
                }

                match plans.len() {
                    0 => {
                        // not a supported case for this optimizer rule
                        Ok(plan.clone())
                    }
                    1 => Ok(plans[0].clone()),
                    _ => {
                        // join all of the plans
                        let mut builder = LogicalPlanBuilder::from(plans[0].clone());
                        for plan in plans.iter().skip(1) {
                            builder = builder.cross_join(plan)?;
                        }
                        let join_plan = builder.build()?;
                        trace!("{}", join_plan.display_indent_schema());
                        Ok(join_plan)
                    }
                }
            }
            _ => Ok(plan),
        }
    }

    fn name(&self) -> &str {
        "eliminate_agg_distinct"
    }
}

#[allow(clippy::too_many_arguments)]
fn create_plan(
    plan: &LogicalPlan,
    input: &Arc<LogicalPlan>,
    expr: &Expr,
    group_expr: &Vec<Expr>,
    distinct_columns: &HashSet<Expr>,
    not_distinct_columns: &HashSet<Expr>,
    optimizer_config: &mut OptimizerConfig,
) -> Result<LogicalPlan> {
    let _distinct_columns = unique_set_without_aliases(distinct_columns);
    let _not_distinct_columns = unique_set_without_aliases(not_distinct_columns);
    let has_distinct = _distinct_columns.contains(expr);
    let has_non_distinct = _not_distinct_columns.contains(expr);
    assert!(has_distinct || has_non_distinct);

    let distinct_columns: Vec<Expr> = distinct_columns
        .iter()
        .filter(|e| match e {
            Expr::Alias(x, _) => x.as_ref() == expr,
            other => *other == expr,
        })
        .cloned()
        .collect();

    let not_distinct_columns: Vec<Expr> = not_distinct_columns
        .iter()
        .filter(|e| match e {
            Expr::Alias(x, _) => x.as_ref() == expr,
            other => *other == expr,
        })
        .cloned()
        .collect();

    let distinct_expr: Vec<Expr> = to_sorted_vec(distinct_columns);
    let not_distinct_expr: Vec<Expr> = to_sorted_vec(not_distinct_columns);

    if has_distinct && has_non_distinct && distinct_expr.len() == 1 && not_distinct_expr.len() == 1
    {
        // Projection: SUM(alias2) AS COUNT(a), COUNT(alias1) AS COUNT(DISTINCT a)
        //   Aggregate: groupBy=[[]], aggr=[[SUM(alias2), COUNT(alias1)]]
        //     Aggregate: groupBy=[[a AS alias1]], aggr=[[COUNT(*) AS alias2]]
        //       TableScan: test projection=[a]

        // The first aggregate groups by the distinct expression and performs a COUNT(*). This
        // is the equivalent of `SELECT expr, COUNT(1) GROUP BY expr`.
        let first_aggregate = {
            let mut group_expr = group_expr.clone();
            group_expr.push(expr.clone());
            let alias = format!("__dask_sql_count__{}", optimizer_config.next_id());
            let expr_name = expr.canonical_name();
            let count_expr = Expr::Column(Column::from_qualified_name(&expr_name));
            let aggr_expr = vec![count(count_expr).alias(&alias)];
            LogicalPlan::Aggregate(Aggregate::try_new(input.clone(), group_expr, aggr_expr)?)
        };

        trace!("first agg:\n{}", first_aggregate.display_indent_schema());

        // The second aggregate both sums and counts the number of values returned by the
        // first aggregate
        let second_aggregate = {
            let input_schema = first_aggregate.schema();
            let offset = group_expr.len();
            let sum = Expr::AggregateFunction {
                fun: AggregateFunction::Sum,
                args: vec![col(&input_schema.field(offset + 1).qualified_name())],
                distinct: false,
                filter: None,
            };
            let count = Expr::AggregateFunction {
                fun: AggregateFunction::Count,
                args: vec![col(&input_schema.field(offset).qualified_name())],
                distinct: false,
                filter: None,
            };
            let aggr_expr = vec![sum, count];

            trace!("aggr_expr = {:?}", aggr_expr);

            LogicalPlan::Aggregate(Aggregate::try_new(
                Arc::new(first_aggregate),
                group_expr.clone(),
                aggr_expr,
            )?)
        };

        trace!("second agg:\n{}", second_aggregate.display_indent_schema());

        // wrap in a projection to alias the SUM() back to a COUNT(), and the COUNT() back to
        // a COUNT(DISTINCT), also taking aliases into account
        let projection = {
            let count_col = col(&second_aggregate.schema().field(0).qualified_name());
            let alias_str = format!("COUNT({})", expr);
            let alias_str = alias_str.replace('#', ""); // TODO remove this ugly hack
            let count_col = match &not_distinct_expr[0] {
                Expr::Alias(_, alias) => count_col.alias(alias.as_str()),
                _ => count_col.alias(&alias_str),
            };

            let count_distinct_col = col(&second_aggregate.schema().field(1).qualified_name());
            let count_distinct_col = match &distinct_expr[0] {
                Expr::Alias(_, alias) => count_distinct_col.alias(alias.as_str()),
                expr => {
                    let alias_str = format!("COUNT(DISTINCT {})", expr);
                    let alias_str = alias_str.replace('#', ""); // TODO remove this ugly hack
                    count_distinct_col.alias(&alias_str)
                }
            };

            let mut projected_cols = group_expr.clone();
            projected_cols.push(count_col);
            projected_cols.push(count_distinct_col);

            LogicalPlan::Projection(Projection::try_new(
                projected_cols,
                Arc::new(second_aggregate),
                None,
            )?)
        };

        Ok(projection)
    } else if has_distinct && distinct_expr.len() == 1 {
        // simple case of a single DISTINCT aggregation
        //
        // Projection: COUNT(a) AS COUNT(DISTINCT a)
        //   Aggregate: groupBy=[[]], aggr=[[COUNT(a)]]
        //     Aggregate: groupBy=[[a]], aggr=[[]]
        //       TableScan: test projection=[a]

        // The first aggregate groups by the distinct expression. This is the equivalent
        // of `SELECT DISTINCT expr`.
        let first_aggregate = {
            let mut group_expr = group_expr.clone();
            group_expr.push(expr.clone());
            LogicalPlan::Aggregate(Aggregate::try_new(input.clone(), group_expr, vec![])?)
        };

        trace!("first agg:\n{}", first_aggregate.display_indent_schema());

        // The second aggregate counts the number of values returned by the first aggregate
        let second_aggregate = {
            // Re-create the original Aggregate node without the DISTINCT element
            let count = Expr::AggregateFunction {
                fun: AggregateFunction::Count,
                args: vec![col(&first_aggregate
                    .schema()
                    .field(group_expr.len())
                    .qualified_name())],
                distinct: false,
                filter: None,
            };
            LogicalPlan::Aggregate(Aggregate::try_new(
                Arc::new(first_aggregate),
                group_expr.clone(),
                vec![count],
            )?)
        };

        trace!("second agg:\n{}", second_aggregate.display_indent_schema());

        // wrap in a projection to alias the COUNT() back to a COUNT(DISTINCT) or the
        // user-supplied alias
        let projection = {
            let mut projected_cols = group_expr.clone();
            let count_distinct_col = col(&second_aggregate
                .schema()
                .field(group_expr.len())
                .qualified_name());
            let count_distinct_col = match &distinct_expr[0] {
                Expr::Alias(_, alias) => count_distinct_col.alias(alias.as_str()),
                expr => {
                    let alias_str = format!("COUNT(DISTINCT {})", expr);
                    let alias_str = alias_str.replace('#', ""); // TODO remove this ugly hack
                    count_distinct_col.alias(&alias_str)
                }
            };
            projected_cols.push(count_distinct_col);

            trace!("projected_cols = {:?}", projected_cols);

            LogicalPlan::Projection(Projection::try_new(
                projected_cols,
                Arc::new(second_aggregate),
                None,
            )?)
        };

        Ok(projection)
    } else {
        Ok(plan.clone())
    }
}

fn to_sorted_vec(vec: Vec<Expr>) -> Vec<Expr> {
    let mut vec = vec;
    vec.sort_by(|l, r| format!("{}", l).cmp(&format!("{}", r)));
    vec
}

/// Gather all inputs to COUNT() and COUNT(DISTINCT) aggregate expressions and keep any aliases
fn gather_expressions(
    aggr_expr: &Expr,
    distinct_columns: &mut HashSet<Expr>,
    not_distinct_columns: &mut HashSet<Expr>,
) {
    match aggr_expr {
        Expr::Alias(x, alias) => {
            if let Expr::AggregateFunction {
                fun: AggregateFunction::Count,
                args,
                distinct,
                ..
            } = x.as_ref()
            {
                if *distinct {
                    for arg in args {
                        distinct_columns.insert(arg.clone().alias(alias));
                    }
                } else {
                    for arg in args {
                        not_distinct_columns.insert(arg.clone().alias(alias));
                    }
                }
            }
        }
        Expr::AggregateFunction {
            fun: AggregateFunction::Count,
            args,
            distinct,
            ..
        } => {
            if *distinct {
                for arg in args {
                    distinct_columns.insert(arg.clone());
                }
            } else {
                for arg in args {
                    not_distinct_columns.insert(arg.clone());
                }
            }
        }
        _ => {}
    }
}

fn strip_aliases(expr: &[&Expr]) -> Vec<Expr> {
    expr.iter()
        .cloned()
        .map(|e| match e {
            Expr::Alias(x, _) => x.as_ref().clone(),
            other => other.clone(),
        })
        .collect()
}

fn unique_set_without_aliases(unique_expressions: &HashSet<Expr>) -> HashSet<Expr> {
    let v = Vec::from_iter(unique_expressions);
    let unique_expressions = strip_aliases(v.as_slice());
    let unique_expressions: HashSet<Expr> = HashSet::from_iter(unique_expressions.iter().cloned());
    unique_expressions
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::{
        col,
        count,
        count_distinct,
        logical_plan::{builder::LogicalTableSource, LogicalPlanBuilder},
    };

    use super::*;
    use crate::sql::optimizer::DaskSqlOptimizer;

    /// Optimize with just the eliminate_agg_distinct rule
    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = EliminateAggDistinct::new();
        let optimized_plan = rule
            .optimize(plan, &mut OptimizerConfig::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{}", optimized_plan.display_indent());
        assert_eq!(expected, formatted_plan);
    }

    /// Optimize with all of the optimizer rules, including eliminate_agg_distinct
    fn assert_fully_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let optimizer = DaskSqlOptimizer::new(false);
        let optimized_plan = optimizer
            .run_optimizations(plan.clone())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{}", optimized_plan.display_indent());
        assert_eq!(expected, formatted_plan);
    }

    /// Create a LogicalPlanBuilder representing a scan of a table with the provided name and schema.
    /// This is mostly used for testing and documentation.
    pub fn table_scan(
        name: Option<&str>,
        table_schema: &Schema,
        projection: Option<Vec<usize>>,
    ) -> Result<LogicalPlanBuilder> {
        let tbl_schema = Arc::new(table_schema.clone());
        let table_source = Arc::new(LogicalTableSource::new(tbl_schema));
        LogicalPlanBuilder::scan(name.unwrap_or("test"), table_source, projection)
    }

    fn test_table_scan(table_name: &str) -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::UInt32, false),
            Field::new("c", DataType::UInt32, false),
            Field::new("d", DataType::UInt32, false),
        ]);
        table_scan(Some(table_name), &schema, None)
            .expect("creating scan")
            .build()
            .expect("building plan")
    }

    #[test]
    fn test_single_distinct_group_by() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan("a"))
            .aggregate(vec![col("a")], vec![count_distinct(col("b"))])?
            .build()?;

        let expected = "Projection: a.a, COUNT(a.b) AS COUNT(DISTINCT a.b)\
        \n  Aggregate: groupBy=[[a.a]], aggr=[[COUNT(a.b)]]\
        \n    Aggregate: groupBy=[[a.a, a.b]], aggr=[[]]\
        \n      TableScan: a";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn test_single_distinct_group_by_with_alias() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan("a"))
            .aggregate(vec![col("a")], vec![count_distinct(col("b")).alias("cd_b")])?
            .build()?;

        let expected = "Projection: a.a, COUNT(a.b) AS cd_b\
        \n  Aggregate: groupBy=[[a.a]], aggr=[[COUNT(a.b)]]\
        \n    Aggregate: groupBy=[[a.a, a.b]], aggr=[[]]\
        \n      TableScan: a";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn test_single_distinct_no_group_by() -> Result<()> {
        let empty_group_expr: Vec<Expr> = vec![];
        let plan = LogicalPlanBuilder::from(test_table_scan("a"))
            .aggregate(empty_group_expr, vec![count_distinct(col("a"))])?
            .build()?;

        let expected = "Projection: COUNT(a.a) AS COUNT(DISTINCT a.a)\
        \n  Aggregate: groupBy=[[]], aggr=[[COUNT(a.a)]]\
        \n    Aggregate: groupBy=[[a.a]], aggr=[[]]\
        \n      TableScan: a";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn test_single_distinct_no_group_by_with_alias() -> Result<()> {
        let empty_group_expr: Vec<Expr> = vec![];
        let plan = LogicalPlanBuilder::from(test_table_scan("a"))
            .aggregate(
                empty_group_expr,
                vec![count_distinct(col("a")).alias("cd_a")],
            )?
            .build()?;

        let expected = "Projection: COUNT(a.a) AS cd_a\
        \n  Aggregate: groupBy=[[]], aggr=[[COUNT(a.a)]]\
        \n    Aggregate: groupBy=[[a.a]], aggr=[[]]\
        \n      TableScan: a";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn test_count_and_distinct_group_by() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan("a"))
            .aggregate(
                vec![col("b")],
                vec![count(col("a")), count_distinct(col("a"))],
            )?
            .build()?;

        let expected =
            "Projection: a.b, a.b AS COUNT(a.a), SUM(__dask_sql_count__1) AS COUNT(DISTINCT a.a)\
        \n  Aggregate: groupBy=[[a.b]], aggr=[[SUM(__dask_sql_count__1), COUNT(a.a)]]\
        \n    Aggregate: groupBy=[[a.b, a.a]], aggr=[[COUNT(a.a) AS __dask_sql_count__1]]\
        \n      TableScan: a";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn test_count_and_distinct_no_group_by() -> Result<()> {
        let empty_group_expr: Vec<Expr> = vec![];
        let plan = LogicalPlanBuilder::from(test_table_scan("a"))
            .aggregate(
                empty_group_expr,
                vec![count(col("a")), count_distinct(col("a"))],
            )?
            .build()?;

        let expected =
            "Projection: SUM(__dask_sql_count__1) AS COUNT(a.a), COUNT(a.a) AS COUNT(DISTINCT a.a)\
        \n  Aggregate: groupBy=[[]], aggr=[[SUM(__dask_sql_count__1), COUNT(a.a)]]\
        \n    Aggregate: groupBy=[[a.a]], aggr=[[COUNT(a.a) AS __dask_sql_count__1]]\
        \n      TableScan: a";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn test_count_and_distinct_no_group_by_with_alias() -> Result<()> {
        let empty_group_expr: Vec<Expr> = vec![];
        let plan = LogicalPlanBuilder::from(test_table_scan("a"))
            .aggregate(
                empty_group_expr,
                vec![
                    count(col("a")).alias("c_a"),
                    count_distinct(col("a")).alias("cd_a"),
                ],
            )?
            .build()?;

        let expected = "Projection: SUM(__dask_sql_count__1) AS c_a, COUNT(a.a) AS cd_a\
        \n  Aggregate: groupBy=[[]], aggr=[[SUM(__dask_sql_count__1), COUNT(a.a)]]\
        \n    Aggregate: groupBy=[[a.a]], aggr=[[COUNT(a.a) AS __dask_sql_count__1]]\
        \n      TableScan: a";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn test_multiple_distinct() -> Result<()> {
        let empty_group_expr: Vec<Expr> = vec![];
        let plan = LogicalPlanBuilder::from(test_table_scan("a"))
            .aggregate(
                empty_group_expr,
                vec![
                    count(col("a")),
                    count_distinct(col("a")),
                    count(col("b")),
                    count_distinct(col("b")),
                    count(col("c")),
                    count_distinct(col("c")),
                    count(col("d")),
                    count_distinct(col("d")),
                ],
            )?
            .build()?;

        let expected = "CrossJoin:\
        \n  CrossJoin:\
        \n    CrossJoin:\
        \n      Projection: SUM(__dask_sql_count__1) AS COUNT(a.a), COUNT(a.a) AS COUNT(DISTINCT a.a)\
        \n        Aggregate: groupBy=[[]], aggr=[[SUM(__dask_sql_count__1), COUNT(a.a)]]\
        \n          Aggregate: groupBy=[[a.a]], aggr=[[COUNT(a.a) AS __dask_sql_count__1]]\
        \n            TableScan: a\
        \n      Projection: SUM(__dask_sql_count__2) AS COUNT(a.b), COUNT(a.b) AS COUNT(DISTINCT a.b)\
        \n        Aggregate: groupBy=[[]], aggr=[[SUM(__dask_sql_count__2), COUNT(a.b)]]\
        \n          Aggregate: groupBy=[[a.b]], aggr=[[COUNT(a.b) AS __dask_sql_count__2]]\
        \n            TableScan: a\
        \n    Projection: SUM(__dask_sql_count__3) AS COUNT(a.c), COUNT(a.c) AS COUNT(DISTINCT a.c)\
        \n      Aggregate: groupBy=[[]], aggr=[[SUM(__dask_sql_count__3), COUNT(a.c)]]\
        \n        Aggregate: groupBy=[[a.c]], aggr=[[COUNT(a.c) AS __dask_sql_count__3]]\
        \n          TableScan: a\
        \n  Projection: SUM(__dask_sql_count__4) AS COUNT(a.d), COUNT(a.d) AS COUNT(DISTINCT a.d)\
        \n    Aggregate: groupBy=[[]], aggr=[[SUM(__dask_sql_count__4), COUNT(a.d)]]\
        \n      Aggregate: groupBy=[[a.d]], aggr=[[COUNT(a.d) AS __dask_sql_count__4]]\
        \n        TableScan: a";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn test_multiple_distinct_with_aliases() -> Result<()> {
        let empty_group_expr: Vec<Expr> = vec![];
        let plan = LogicalPlanBuilder::from(test_table_scan("a"))
            .aggregate(
                empty_group_expr,
                vec![
                    count(col("a")).alias("c_a"),
                    count_distinct(col("a")).alias("cd_a"),
                    count(col("b")).alias("c_b"),
                    count_distinct(col("b")).alias("cd_b"),
                    count(col("c")).alias("c_c"),
                    count_distinct(col("c")).alias("cd_c"),
                    count(col("d")).alias("c_d"),
                    count_distinct(col("d")).alias("cd_d"),
                ],
            )?
            .build()?;

        let expected = "CrossJoin:\
        \n  CrossJoin:\
        \n    CrossJoin:\
        \n      Projection: SUM(__dask_sql_count__1) AS c_a, COUNT(a.a) AS cd_a\
        \n        Aggregate: groupBy=[[]], aggr=[[SUM(__dask_sql_count__1), COUNT(a.a)]]\
        \n          Aggregate: groupBy=[[a.a]], aggr=[[COUNT(a.a) AS __dask_sql_count__1]]\
        \n            TableScan: a\
        \n      Projection: SUM(__dask_sql_count__2) AS c_b, COUNT(a.b) AS cd_b\
        \n        Aggregate: groupBy=[[]], aggr=[[SUM(__dask_sql_count__2), COUNT(a.b)]]\
        \n          Aggregate: groupBy=[[a.b]], aggr=[[COUNT(a.b) AS __dask_sql_count__2]]\
        \n            TableScan: a\
        \n    Projection: SUM(__dask_sql_count__3) AS c_c, COUNT(a.c) AS cd_c\
        \n      Aggregate: groupBy=[[]], aggr=[[SUM(__dask_sql_count__3), COUNT(a.c)]]\
        \n        Aggregate: groupBy=[[a.c]], aggr=[[COUNT(a.c) AS __dask_sql_count__3]]\
        \n          TableScan: a\
        \n  Projection: SUM(__dask_sql_count__4) AS c_d, COUNT(a.d) AS cd_d\
        \n    Aggregate: groupBy=[[]], aggr=[[SUM(__dask_sql_count__4), COUNT(a.d)]]\
        \n      Aggregate: groupBy=[[a.d]], aggr=[[COUNT(a.d) AS __dask_sql_count__4]]\
        \n        TableScan: a";
        assert_optimized_plan_eq(&plan, expected);

        let expected = "CrossJoin:\
        \n  CrossJoin:\
        \n    CrossJoin:\
        \n      Projection: SUM(__dask_sql_count__1) AS c_a, COUNT(a.a) AS cd_a\
        \n        Aggregate: groupBy=[[]], aggr=[[SUM(__dask_sql_count__1), COUNT(a.a)]]\
        \n          Aggregate: groupBy=[[a.a]], aggr=[[COUNT(a.a) AS __dask_sql_count__1]]\
        \n            TableScan: a projection=[a]\
        \n      Projection: SUM(__dask_sql_count__2) AS c_b, COUNT(a.b) AS cd_b\
        \n        Aggregate: groupBy=[[]], aggr=[[SUM(__dask_sql_count__2), COUNT(a.b)]]\
        \n          Aggregate: groupBy=[[a.b]], aggr=[[COUNT(a.b) AS __dask_sql_count__2]]\
        \n            TableScan: a projection=[b]\
        \n    Projection: SUM(__dask_sql_count__3) AS c_c, COUNT(a.c) AS cd_c\
        \n      Aggregate: groupBy=[[]], aggr=[[SUM(__dask_sql_count__3), COUNT(a.c)]]\
        \n        Aggregate: groupBy=[[a.c]], aggr=[[COUNT(a.c) AS __dask_sql_count__3]]\
        \n          TableScan: a projection=[c]\
        \n  Projection: SUM(__dask_sql_count__4) AS c_d, COUNT(a.d) AS cd_d\
        \n    Aggregate: groupBy=[[]], aggr=[[SUM(__dask_sql_count__4), COUNT(a.d)]]\
        \n      Aggregate: groupBy=[[a.d]], aggr=[[COUNT(a.d) AS __dask_sql_count__4]]\
        \n        TableScan: a projection=[d]";
        assert_fully_optimized_plan_eq(&plan, expected);

        Ok(())
    }
}
