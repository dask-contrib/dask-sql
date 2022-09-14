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
//! Projection: #COUNT(a.a) AS COUNT(DISTINCT #a.a)))\
//!   Aggregate: groupBy=[[]], aggr=[[COUNT(#a.a)]]\
//!     Aggregate: groupBy=[[#a.a]], aggr=[[]]\
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
//! Projection: #SUM(alias2) AS COUNT(a), #COUNT(alias1) AS COUNT(DISTINCT a)
//!   Aggregate: groupBy=[[]], aggr=[[SUM(alias2), COUNT(#alias1)]]
//!     Aggregate: groupBy=[[#a AS alias1]], aggr=[[COUNT(*) AS alias2]]
//!       TableScan: test projection=[a]
//!
//! If the query contains DISTINCT aggregates for multiple columns then we need to perform
//! separate aggregate queries per column and then join the results. The final Dask plan
//! should like like this:
//!
//! CrossJoin:\
//!  CrossJoin:\
//!    CrossJoin:\
//!      Projection: #SUM(__dask_sql_count__1) AS COUNT(#a.a), #COUNT(a.a) AS COUNT(DISTINCT #a.a)\
//!        Aggregate: groupBy=[[]], aggr=[[SUM(#__dask_sql_count__1), COUNT(#a.a)]]\
//!          Aggregate: groupBy=[[#a.a]], aggr=[[COUNT(UInt64(1)) AS __dask_sql_count__1]]\
//!            TableScan: a\
//!      Projection: #SUM(__dask_sql_count__2) AS COUNT(#a.b), #COUNT(a.b) AS COUNT(DISTINCT(#a.b))\
//!        Aggregate: groupBy=[[]], aggr=[[SUM(#__dask_sql_count__2), COUNT(#a.b)]]\
//!          Aggregate: groupBy=[[#a.b]], aggr=[[COUNT(UInt64(1)) AS __dask_sql_count__2]]\
//!            TableScan: a\
//!    Projection: #SUM(__dask_sql_count__3) AS COUNT(#a.c), #COUNT(a.c) AS COUNT(DISTINCT(#a.c))\
//!      Aggregate: groupBy=[[]], aggr=[[SUM(#__dask_sql_count__3), COUNT(#a.c)]]\
//!        Aggregate: groupBy=[[#a.c]], aggr=[[COUNT(UInt64(1)) AS __dask_sql_count__3]]\
//!          TableScan: a\
//!  Projection: #SUM(__dask_sql_count__4) AS COUNT(#a.d), #COUNT(a.d) AS COUNT(DISTINCT(#a.d))\
//!    Aggregate: groupBy=[[]], aggr=[[SUM(#__dask_sql_count__4), COUNT(#a.d)]]\
//!      Aggregate: groupBy=[[#a.d]], aggr=[[COUNT(UInt64(1)) AS __dask_sql_count__4]]\
//!        TableScan: a

use datafusion_common::{Column, DFSchema, Result, ScalarValue};
use datafusion_expr::logical_plan::Projection;
use datafusion_expr::utils::exprlist_to_fields;
use datafusion_expr::{
    col, count,
    logical_plan::{Aggregate, LogicalPlan},
    AggregateFunction, Expr, LogicalPlanBuilder,
};
use datafusion_optimizer::{utils, OptimizerConfig, OptimizerRule};
use std::collections::hash_map::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

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

                let group_expr = strip_qualifiers(&group_expr);

                let plans: Vec<LogicalPlan> = x
                    .iter()
                    .map(|expr| {
                        create_plan(
                            &plan,
                            input,
                            expr,
                            &group_expr,
                            &distinct_columns,
                            &not_distinct_columns,
                            optimizer_config,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

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
                        builder.build()
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
        // Projection: #SUM(alias2) AS COUNT(a), #COUNT(alias1) AS COUNT(DISTINCT a)
        //   Aggregate: groupBy=[[]], aggr=[[SUM(alias2), COUNT(#alias1)]]
        //     Aggregate: groupBy=[[#a AS alias1]], aggr=[[COUNT(*) AS alias2]]
        //       TableScan: test projection=[a]

        // The first aggregate groups by the distinct expression and performs a COUNT(*). This
        // is the equivalent of `SELECT expr, COUNT(1) GROUP BY expr`.
        let first_aggregate = {
            let mut group_expr = group_expr.clone();
            group_expr.push(strip_qualifier(expr));
            let alias = format!("__dask_sql_count__{}", optimizer_config.next_id());
            let aggr_expr = vec![count(Expr::Literal(ScalarValue::UInt64(Some(1)))).alias(&alias)];
            let mut schema_expr = group_expr.clone();
            schema_expr.extend_from_slice(&aggr_expr);
            let schema = DFSchema::new_with_metadata(
                exprlist_to_fields(&schema_expr, input)?,
                HashMap::new(),
            )?;
            LogicalPlan::Aggregate(Aggregate::try_new(
                input.clone(),
                group_expr,
                aggr_expr,
                Arc::new(schema),
            )?)
        };

        println!("first agg:\n{}", first_aggregate.display_indent_schema());

        // The second aggregate both sums and counts the number of values returned by the
        // first aggregate
        let second_aggregate = {
            let input_schema = first_aggregate.schema();
            let offset = group_expr.len();
            let sum = Expr::AggregateFunction {
                fun: AggregateFunction::Sum,
                args: vec![col(&input_schema.field(offset + 1).name())],
                distinct: false,
                filter: None,
            };
            let count = Expr::AggregateFunction {
                fun: AggregateFunction::Count,
                args: vec![col(&input_schema.field(offset).name())],
                distinct: false,
                filter: None,
            };
            let aggr_expr = vec![sum, count];

            println!("aggr_expr = {:?}", aggr_expr);

            let mut schema_expr = group_expr.clone();
            schema_expr.extend_from_slice(&aggr_expr);
            let schema_expr = strip_qualifiers(&schema_expr);
            let fields = exprlist_to_fields(&schema_expr, &first_aggregate)?;
            let schema = DFSchema::new_with_metadata(fields, HashMap::new())?;
            LogicalPlan::Aggregate(Aggregate::try_new(
                Arc::new(first_aggregate),
                group_expr.clone(),
                aggr_expr,
                Arc::new(schema),
            )?)
        };

        println!("second agg:\n{}", second_aggregate.display_indent_schema());

        // wrap in a projection to alias the SUM() back to a COUNT(), and the COUNT() back to
        // a COUNT(DISTINCT), also taking aliases into account
        let projection = {
            let count_col = col(&second_aggregate.schema().field(0).qualified_name());
            let alias_str = format!("COUNT({})", strip_qualifier(expr));
            let alias_str = alias_str.replace("#", ""); // TODO remove this ugly hack
            let count_col = match &not_distinct_expr[0] {
                Expr::Alias(_, alias) => count_col.alias(alias.as_str()),
                _ => count_col.alias(&alias_str),
            };

            let count_distinct_col = col(&second_aggregate.schema().field(1).qualified_name());
            let count_distinct_col = match &distinct_expr[0] {
                Expr::Alias(_, alias) => count_distinct_col.alias(alias.as_str()),
                expr => {
                    let alias_str = format!("COUNT(DISTINCT {})", strip_qualifier(expr));
                    let alias_str = alias_str.replace("#", ""); // TODO remove this ugly hack
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
        // Projection: #COUNT(#a) AS COUNT(DISTINCT a)
        //   Aggregate: groupBy=[[]], aggr=[[COUNT(#a)]]
        //     Aggregate: groupBy=[[#a]], aggr=[[]]
        //       TableScan: test projection=[a]

        // The first aggregate groups by the distinct expression. This is the equivalent
        // of `SELECT DISTINCT expr`.
        let first_aggregate = {
            let mut group_expr = group_expr.clone();
            group_expr.push(strip_qualifier(expr));
            let fields = exprlist_to_fields(&group_expr, input)?;
            let schema = DFSchema::new_with_metadata(fields, HashMap::new())?;
            LogicalPlan::Aggregate(Aggregate::try_new(
                input.clone(),
                group_expr,
                vec![],
                Arc::new(schema),
            )?)
        };

        println!("first agg:\n{}", first_aggregate.display_indent_schema());

        // The second aggregate counts the number of values returned by the first aggregate
        let second_aggregate = {
            // Re-create the original Aggregate node without the DISTINCT element
            let count = Expr::AggregateFunction {
                fun: AggregateFunction::Count,
                args: vec![col(&first_aggregate.schema().field(0).qualified_name())],
                distinct: false,
                filter: None,
            };
            let mut second_aggr_schema = group_expr.clone();
            second_aggr_schema.push(count.clone());
            let fields = exprlist_to_fields(&second_aggr_schema, &first_aggregate)?;
            let fields = fields
                .iter()
                .map(|field| {
                    let field = field.clone();
                    field.strip_qualifier()
                })
                .collect();
            let schema = DFSchema::new_with_metadata(fields, HashMap::new())?;
            LogicalPlan::Aggregate(Aggregate::try_new(
                Arc::new(first_aggregate),
                group_expr.clone(),
                vec![count],
                Arc::new(schema),
            )?)
        };

        println!("second agg:\n{}", second_aggregate.display_indent_schema());

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
                    let alias_str = format!("COUNT(DISTINCT {})", strip_qualifier(expr));
                    let alias_str = alias_str.replace("#", ""); // TODO remove this ugly hack
                    count_distinct_col.alias(&alias_str)
                }
            };
            projected_cols.push(count_distinct_col);

            println!("projected_cols = {:?}", projected_cols);

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

fn strip_qualifiers(expr: &Vec<Expr>) -> Vec<Expr> {
    expr.iter().map(strip_qualifier).collect()
}

fn strip_qualifier(expr: &Expr) -> Expr {
    match expr {
        Expr::Column(col) => Expr::Column(Column::from_name(&col.name)),
        Expr::Alias(expr, alias) => Expr::Alias(Box::new(strip_qualifier(expr)), alias.clone()),
        Expr::AggregateFunction {
            fun,
            args,
            distinct,
            filter,
        } => Expr::AggregateFunction {
            fun: fun.clone(),
            args: strip_qualifiers(args),
            distinct: *distinct,
            filter: filter.clone(),
        },
        _ => {
            println!("cannot strip from {}", expr);
            expr.clone()
        }
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
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::{
        col, count, count_distinct,
        logical_plan::{builder::LogicalTableSource, LogicalPlanBuilder},
    };
    use std::sync::Arc;

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = EliminateAggDistinct::new();
        let optimized_plan = rule
            .optimize(plan, &mut OptimizerConfig::new())
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

        let expected = "Projection: #a, #COUNT(a) AS COUNT(DISTINCT b)\
        \n  Aggregate: groupBy=[[#a]], aggr=[[COUNT(#a)]]\
        \n    Aggregate: groupBy=[[#a, #b]], aggr=[[]]\
        \n      TableScan: a";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn test_single_distinct_group_by_with_alias() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan("a"))
            .aggregate(vec![col("a")], vec![count_distinct(col("b")).alias("cd_b")])?
            .build()?;

        let expected = "Projection: #a, #COUNT(a) AS cd_b\
        \n  Aggregate: groupBy=[[#a]], aggr=[[COUNT(#a)]]\
        \n    Aggregate: groupBy=[[#a, #b]], aggr=[[]]\
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

        let expected = "Projection: #COUNT(a) AS COUNT(DISTINCT a)\
        \n  Aggregate: groupBy=[[]], aggr=[[COUNT(#a)]]\
        \n    Aggregate: groupBy=[[#a]], aggr=[[]]\
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

        let expected = "Projection: #COUNT(a) AS cd_a\
        \n  Aggregate: groupBy=[[]], aggr=[[COUNT(#a)]]\
        \n    Aggregate: groupBy=[[#a]], aggr=[[]]\
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
            "Projection: #b, #b AS COUNT(a), #SUM(__dask_sql_count__1) AS COUNT(DISTINCT a)\
        \n  Aggregate: groupBy=[[#b]], aggr=[[SUM(#__dask_sql_count__1), COUNT(#a)]]\
        \n    Aggregate: groupBy=[[#b, #a]], aggr=[[COUNT(UInt64(1)) AS __dask_sql_count__1]]\
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
            "Projection: #SUM(__dask_sql_count__1) AS COUNT(a), #COUNT(a) AS COUNT(DISTINCT a)\
        \n  Aggregate: groupBy=[[]], aggr=[[SUM(#__dask_sql_count__1), COUNT(#a)]]\
        \n    Aggregate: groupBy=[[#a]], aggr=[[COUNT(UInt64(1)) AS __dask_sql_count__1]]\
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

        let expected = "Projection: #SUM(__dask_sql_count__1) AS c_a, #COUNT(a) AS cd_a\
        \n  Aggregate: groupBy=[[]], aggr=[[SUM(#__dask_sql_count__1), COUNT(#a)]]\
        \n    Aggregate: groupBy=[[#a]], aggr=[[COUNT(UInt64(1)) AS __dask_sql_count__1]]\
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
        \n      Projection: #SUM(__dask_sql_count__1) AS COUNT(#a.a), #COUNT(a.a) AS COUNT(DISTINCT #a.a)\
        \n        Aggregate: groupBy=[[]], aggr=[[SUM(#__dask_sql_count__1), COUNT(#a.a)]]\
        \n          Aggregate: groupBy=[[#a.a]], aggr=[[COUNT(UInt64(1)) AS __dask_sql_count__1]]\
        \n            TableScan: a\
        \n      Projection: #SUM(__dask_sql_count__2) AS COUNT(#a.b), #COUNT(a.b) AS COUNT(DISTINCT #a.b)\
        \n        Aggregate: groupBy=[[]], aggr=[[SUM(#__dask_sql_count__2), COUNT(#a.b)]]\
        \n          Aggregate: groupBy=[[#a.b]], aggr=[[COUNT(UInt64(1)) AS __dask_sql_count__2]]\
        \n            TableScan: a\
        \n    Projection: #SUM(__dask_sql_count__3) AS COUNT(#a.c), #COUNT(a.c) AS COUNT(DISTINCT #a.c)\
        \n      Aggregate: groupBy=[[]], aggr=[[SUM(#__dask_sql_count__3), COUNT(#a.c)]]\
        \n        Aggregate: groupBy=[[#a.c]], aggr=[[COUNT(UInt64(1)) AS __dask_sql_count__3]]\
        \n          TableScan: a\
        \n  Projection: #SUM(__dask_sql_count__4) AS COUNT(#a.d), #COUNT(a.d) AS COUNT(DISTINCT #a.d)\
        \n    Aggregate: groupBy=[[]], aggr=[[SUM(#__dask_sql_count__4), COUNT(#a.d)]]\
        \n      Aggregate: groupBy=[[#a.d]], aggr=[[COUNT(UInt64(1)) AS __dask_sql_count__4]]\
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
        \n      Projection: #SUM(__dask_sql_count__1) AS c_a, #COUNT(a.a) AS cd_a\
        \n        Aggregate: groupBy=[[]], aggr=[[SUM(#__dask_sql_count__1), COUNT(#a.a)]]\
        \n          Aggregate: groupBy=[[#a.a]], aggr=[[COUNT(UInt64(1)) AS __dask_sql_count__1]]\
        \n            TableScan: a\
        \n      Projection: #SUM(__dask_sql_count__2) AS c_b, #COUNT(a.b) AS cd_b\
        \n        Aggregate: groupBy=[[]], aggr=[[SUM(#__dask_sql_count__2), COUNT(#a.b)]]\
        \n          Aggregate: groupBy=[[#a.b]], aggr=[[COUNT(UInt64(1)) AS __dask_sql_count__2]]\
        \n            TableScan: a\
        \n    Projection: #SUM(__dask_sql_count__3) AS c_c, #COUNT(a.c) AS cd_c\
        \n      Aggregate: groupBy=[[]], aggr=[[SUM(#__dask_sql_count__3), COUNT(#a.c)]]\
        \n        Aggregate: groupBy=[[#a.c]], aggr=[[COUNT(UInt64(1)) AS __dask_sql_count__3]]\
        \n          TableScan: a\
        \n  Projection: #SUM(__dask_sql_count__4) AS c_d, #COUNT(a.d) AS cd_d\
        \n    Aggregate: groupBy=[[]], aggr=[[SUM(#__dask_sql_count__4), COUNT(#a.d)]]\
        \n      Aggregate: groupBy=[[#a.d]], aggr=[[COUNT(UInt64(1)) AS __dask_sql_count__4]]\
        \n        TableScan: a";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }
}
