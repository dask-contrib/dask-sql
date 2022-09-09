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
//! Projection: #COUNT(DISTINCT a)
//!   Projection: #COUNT(alias1) AS COUNT(DISTINCT a)
//!     Aggregate: groupBy=[[]], aggr=[[COUNT(#alias1)]]
//!       Aggregate: groupBy=[[#a AS alias1]], aggr=[[]]
//!         TableScan: test projection=[a]
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
//! Projection: #COUNT(DISTINCT a)
//!   Projection: #SUM(alias2) AS COUNT(a), #COUNT(alias1) AS COUNT(DISTINCT a)
//!     Aggregate: groupBy=[[]], aggr=[[SUM(alias2), COUNT(#alias1)]]
//!       Aggregate: groupBy=[[#a AS alias1]], aggr=[[COUNT(*) AS alias2]]
//!         TableScan: test projection=[a]
//!
//! If the query contains DISTICT aggregates for multiple columns then we need to perform
//! separate aggregate queries per column and then join the results.
//!
//! DaskProject(c_a=[$0], cd_a=[$5], c_b=[$1], cd_b=[$6], c_c=[$2], cd_c=[$7], c_d=[$3], cd_d=[$8], c_e=[$4], cd_e=[$5]): rowcount = 1.0, cumulative cost = {965.75 rows, 915.0 cpu, 0.0 io}, id = 477
//!   DaskJoin(condition=[true], joinType=[inner]): rowcount = 1.0, cumulative cost = {964.75 rows, 905.0 cpu, 0.0 io}, id = 476
//!     DaskJoin(condition=[true], joinType=[inner]): rowcount = 1.0, cumulative cost = {752.625 rows, 704.0 cpu, 0.0 io}, id = 472
//!       DaskJoin(condition=[true], joinType=[inner]): rowcount = 1.0, cumulative cost = {540.5 rows, 503.0 cpu, 0.0 io}, id = 468
//!         DaskJoin(condition=[true], joinType=[inner]): rowcount = 1.0, cumulative cost = {328.375 rows, 302.0 cpu, 0.0 io}, id = 464
//!           DaskAggregate(group=[{}], c_a=[COUNT($0)], c_b=[COUNT($1)], c_c=[COUNT($2)], c_d=[COUNT($3)], c_e=[COUNT($4)]): rowcount = 10.0, cumulative cost = {116.25 rows, 101.0 cpu, 0.0 io}, id = 460
//!             DaskTableScan(table=[[root, a]]): rowcount = 100.0, cumulative cost = {100.0 rows, 101.0 cpu, 0.0 io}, id = 363
//!           DaskAggregate(group=[{}], cd_e=[COUNT($0)]): rowcount = 1.0, cumulative cost = {211.125 rows, 201.0 cpu, 0.0 io}, id = 463
//!             DaskAggregate(group=[{0}]): rowcount = 10.0, cumulative cost = {210.0 rows, 201.0 cpu, 0.0 io}, id = 462
//!               DaskProject(a=[$0]): rowcount = 100.0, cumulative cost = {200.0 rows, 201.0 cpu, 0.0 io}, id = 461
//!                 DaskTableScan(table=[[root, a]]): rowcount = 100.0, cumulative cost = {100.0 rows, 101.0 cpu, 0.0 io}, id = 363
//!         DaskAggregate(group=[{}], cd_b=[COUNT($0)]): rowcount = 1.0, cumulative cost = {211.125 rows, 201.0 cpu, 0.0 io}, id = 467
//!           DaskAggregate(group=[{0}]): rowcount = 10.0, cumulative cost = {210.0 rows, 201.0 cpu, 0.0 io}, id = 466
//!             DaskProject(b=[$1]): rowcount = 100.0, cumulative cost = {200.0 rows, 201.0 cpu, 0.0 io}, id = 465
//!               DaskTableScan(table=[[root, a]]): rowcount = 100.0, cumulative cost = {100.0 rows, 101.0 cpu, 0.0 io}, id = 363
//!       DaskAggregate(group=[{}], cd_c=[COUNT($0)]): rowcount = 1.0, cumulative cost = {211.125 rows, 201.0 cpu, 0.0 io}, id = 471
//!         DaskAggregate(group=[{0}]): rowcount = 10.0, cumulative cost = {210.0 rows, 201.0 cpu, 0.0 io}, id = 470
//!           DaskProject(c=[$2]): rowcount = 100.0, cumulative cost = {200.0 rows, 201.0 cpu, 0.0 io}, id = 469
//!             DaskTableScan(table=[[root, a]]): rowcount = 100.0, cumulative cost = {100.0 rows, 101.0 cpu, 0.0 io}, id = 363
//!     DaskAggregate(group=[{}], cd_d=[COUNT($0)]): rowcount = 1.0, cumulative cost = {211.125 rows, 201.0 cpu, 0.0 io}, id = 475
//!       DaskAggregate(group=[{0}]): rowcount = 10.0, cumulative cost = {210.0 rows, 201.0 cpu, 0.0 io}, id = 474
//!         DaskProject(d=[$3]): rowcount = 100.0, cumulative cost = {200.0 rows, 201.0 cpu, 0.0 io}, id = 473
//!           DaskTableScan(table=[[root, a]]): rowcount = 100.0, cumulative cost = {100.0 rows, 101.0 cpu, 0.0 io}, id = 363

use datafusion_common::{DFField, DFSchema, Result};
use datafusion_expr::{
    logical_plan::{Aggregate, LogicalPlan},
    Expr,
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
        println!("Incoming LogicalPlan: {:?}", plan);

        //TODO should optimize inputs first

        match plan {
            LogicalPlan::Aggregate(Aggregate {
                input,
                group_expr: _,
                aggr_expr,
                schema,
            }) => {
                // Original plan
                let mut original_plan = plan.clone();
                let mut optimized_node: bool = false;

                let mut updated_schema: Option<DFSchema> = None;

                let mut optimized_aggr_expr: Vec<Expr> = Vec::new();

                let mut distinct_column: Option<Expr> = None;

                for ex in aggr_expr {
                    match ex {
                        Expr::AggregateFunction {
                            fun,
                            args,
                            distinct,
                        } => {
                            // Optimize away COUNT(DISTINCT(col)) into another AGGREGATE input of the original LogicalPlan
                            if *distinct {
                                optimized_node = true;
                                optimized_aggr_expr.push(Expr::AggregateFunction {
                                    fun: fun.clone(),
                                    args: args.clone(),
                                    distinct: false,
                                });
                                // Update the DFSchema to rename the field with the DISTINCT in it
                                let mut new_fields: Vec<DFField> = vec![];
                                let mut field_names: HashSet<String> = HashSet::new();
                                for field in schema.fields() {
                                    if field.name().contains("DISTINCT") {
                                        let new_field_name = field.name().replace("DISTINCT ", "");
                                        if field_names.insert(new_field_name.clone()) {
                                            match field.qualifier() {
                                                Some(e) => {
                                                    new_fields.push(DFField::new(
                                                        Some(e),
                                                        new_field_name.as_str(),
                                                        field.data_type().clone(),
                                                        field.is_nullable(),
                                                    ));
                                                }
                                                None => {
                                                    new_fields.push(DFField::new(
                                                        None,
                                                        new_field_name.as_str(),
                                                        field.data_type().clone(),
                                                        field.is_nullable(),
                                                    ));
                                                }
                                            }
                                        }
                                    } else {
                                        if field_names.insert(field.name().clone()) {
                                            new_fields.push(field.clone());
                                        }
                                    }
                                }

                                // Loop through the args
                                for arg in args {
                                    if let Expr::Column(_column) = arg {
                                        distinct_column = Some(arg.clone());
                                    }
                                }

                                updated_schema =
                                    Some(DFSchema::new_with_metadata(new_fields, HashMap::new())?);
                            }
                        }
                        _ => optimized_aggr_expr.push(ex.clone()),
                    }
                }

                if optimized_node {
                    // Update the Schema
                    let mut new_fields: Vec<DFField> = Vec::new();
                    for field in schema.fields() {
                        if !field.name().contains("DISTINCT") {
                            new_fields.push(field.clone());
                        }
                    }

                    let distinct_input = LogicalPlan::Aggregate(Aggregate {
                        input: input.clone(),
                        group_expr: vec![distinct_column.unwrap()],
                        aggr_expr: vec![],
                        schema: Arc::new(DFSchema::new_with_metadata(new_fields, HashMap::new())?),
                    });

                    // Re-create the original Aggregate node without the DISTINCT element
                    original_plan = LogicalPlan::Aggregate(Aggregate {
                        input: Arc::new(distinct_input),
                        group_expr: vec![],
                        aggr_expr: optimized_aggr_expr,
                        schema: Arc::new(updated_schema.unwrap()),
                    });
                }

                Ok(original_plan)
            }
            _ => {
                // Apply the optimization to all inputs of the plan
                println!("Optimize children was invoked!!!");
                utils::optimize_children(self, plan, optimizer_config)
            }
        }
    }

    fn name(&self) -> &str {
        "elimintate_agg_distinct"
    }
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
        let formatted_plan = format!("{}", optimized_plan.display_indent_schema());
        assert_eq!(formatted_plan, expected);
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
    fn test_single_distinct_no_group_by() -> Result<()> {
        let empty_group_expr: Vec<Expr> = vec![];
        let plan = LogicalPlanBuilder::from(test_table_scan("a"))
            .aggregate(
                empty_group_expr,
                vec![count_distinct(col("a")).alias("cd_a")],
            )?
            .build()?;

        let expected = "TBD";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn test_count_and_distinct_no_group_by() -> Result<()> {
        let empty_group_expr: Vec<Expr> = vec![];
        let plan = LogicalPlanBuilder::from(test_table_scan("a"))
            .aggregate(
                empty_group_expr,
                vec![count(col("a")).alias("c_a"), count_distinct(col("a")).alias("cd_a")],
            )?
            .build()?;

        let expected = "TBD";
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
                    count(col("a")).alias("c_a"),
                    count_distinct(col("a")).alias("cd_a"),
                    count(col("b")).alias("c_b"),
                    count_distinct(col("b")).alias("cd_b"),
                    count(col("c")).alias("c_c"),
                    count_distinct(col("c")).alias("cd_c"),
                    count(col("d")).alias("c_d"),
                    count_distinct(col("a")).alias("cd_d"),
                ],
            )?
            .build()?;

        let expected = "TBD";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn count_distinct_with_group_by() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan("test"))
            .aggregate(vec![col("test.b")], vec![count_distinct(col("test.b"))])?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: #test.b [b:UInt32]\
        \n  Aggregate: groupBy=[[]], aggr=[[COUNT(#test.b)]] [b:UInt32, COUNT(test.b):Int64;N]\
        \n    Aggregate: groupBy=[[#test.b]], aggr=[[]] [b:UInt32]\
        \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32, d:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    ///
    ///SELECT
    /// COUNT(b) AS cnt_b,
    /// COUNT(DISTINCT b) AS cntd_b
    ///FROM test
    ///
    #[test]
    fn count_distinct_multi() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan("test"))
            .aggregate(
                vec![col("test.b")],
                vec![count(col("test.b")), count_distinct(col("test.b"))],
            )?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: #test.b [b:UInt32]\
        \n  Aggregate: groupBy=[[]], aggr=[[COUNT(#test.b)]] [b:UInt32, COUNT(test.b):Int64;N]\
        \n    Aggregate: groupBy=[[#test.b]], aggr=[[]] [b:UInt32]\
        \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32, d:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }
}
