//! Optimizer rule eliminating DISTINCT operations contained in AggregateFunction
//! operations
//!
//! It handles standalone parts of logical conjunction expressions, i.e.
//! ```text
//!   WHERE t1.f IN (SELECT f FROM t2) AND t2.f = 'x'
//! ```
//! will be rewritten, but
//! ```text
//!   WHERE t1.f IN (SELECT f FROM t2) OR t2.f = 'x'
//! ```
//! won't
use arrow::datatypes::Schema;
use datafusion_common::{DFField, DFSchema, DataFusionError, Result};
use datafusion_expr::{
    col,
    expr_visitor::{ExprVisitable, ExpressionVisitor, Recursion},
    logical_plan::{
        builder::build_join_schema, Aggregate, Distinct, Filter, Join, JoinConstraint, JoinType,
        LogicalPlan,
    },
    Expr,
    Expr::Column,
};
use datafusion_optimizer::{utils, OptimizerConfig, OptimizerRule};
use std::collections::hash_map::HashMap;
use std::sync::Arc;

/// Optimizer rule for rewriting subquery filters to joins
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
        match plan {
            LogicalPlan::Aggregate(Aggregate {
                input,
                group_expr,
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
                                let mut new_fields: Vec<DFField> = Vec::new();
                                for field in schema.fields() {
                                    if field.name().contains("DISTINCT") {
                                        let result = field.name().replace("DISTINCT ", "");
                                        match field.qualifier() {
                                            Some(e) => {
                                                new_fields.push(DFField::new(
                                                    Some(e),
                                                    result.as_str(),
                                                    field.data_type().clone(),
                                                    field.is_nullable(),
                                                ));
                                            }
                                            None => {
                                                new_fields.push(DFField::new(
                                                    None,
                                                    result.as_str(),
                                                    field.data_type().clone(),
                                                    field.is_nullable(),
                                                ));
                                            }
                                        }
                                    } else {
                                        new_fields.push(field.clone());
                                    }
                                }

                                // Loop through the args
                                for arg in args {
                                    match arg {
                                        Expr::Column(_column) => {
                                            distinct_column = Some(arg.clone());
                                        }
                                        _ => (),
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
                        schema: Arc::new(updated_schema.unwrap().clone()),
                    });
                }

                Ok(original_plan)
            }
            _ => {
                // Apply the optimization to all inputs of the plan
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

    fn test_table_scan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::UInt32, false),
            Field::new("c", DataType::UInt32, false),
            Field::new("d", DataType::UInt32, false),
        ]);
        table_scan(Some("test"), &schema, None)
            .expect("creating scan")
            .build()
            .expect("building plan")
    }

    #[test]
    fn count_distinct_simple() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan())
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
    /// COUNT(a) AS c_a,
    /// COUNT(DISTINCT a) AS cd_a
    ///FROM test
    ///
    #[test]
    fn count_distinct_multi() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan())
            .aggregate(
                vec![col("test.a")],
                vec![count(col("test.a")), count_distinct(col("test.a"))],
            )?
            .project(vec![col("test.a")])?
            .build()?;

        let expected = "Projection: #test.b [b:UInt32]\
        \n  Aggregate: groupBy=[[]], aggr=[[COUNT(#test.b)]] [b:UInt32, COUNT(test.b):Int64;N]\
        \n    Aggregate: groupBy=[[#test.b]], aggr=[[]] [b:UInt32]\
        \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32, d:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }
}
