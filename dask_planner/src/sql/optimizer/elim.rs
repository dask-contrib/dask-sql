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
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{
    expr_visitor::{ExprVisitable, ExpressionVisitor, Recursion},
    logical_plan::{
        builder::build_join_schema, Aggregate, Filter, Join, JoinConstraint, JoinType, LogicalPlan,
    },
    Expr,
};
use datafusion_optimizer::{utils, OptimizerConfig, OptimizerRule};
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
                // Apply optimizer rule to current input
                let optimized_input = self.optimize(input, optimizer_config)?;

                // Examine aggr_expr vec looking for inner DISTINCT operations
                let values: Vec<&Expr> = aggr_expr
                    .iter()
                    .map(|f| {
                        println!("Expr: {:?}", f);
                        println!("Something after this");
                        f
                    })
                    .collect();

                println!("Did this show up?");
                println!("Trigger now: {:?}", values);

                for ex in aggr_expr {
                    match ex {
                        Expr::AggregateFunction {
                            fun,
                            args,
                            distinct,
                        } => {
                            println!("AggregationFunction");
                            if *distinct {
                                panic!("DISTINCT!!!");
                            }
                        }
                        _ => (),
                    }
                }

                Ok(optimized_input)
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
        col, count_distinct,
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
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Boolean, false),
            Field::new("c", DataType::Boolean, false),
            Field::new("d", DataType::UInt32, false),
        ]);
        table_scan(Some("test"), &schema, None)
            .expect("creating scan")
            .build()
            .expect("building plan")
    }

    /// Test for single IN subquery filter
    #[test]
    fn in_subquery_simple() -> Result<()> {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("test.b")], vec![count_distinct(col("test.b"))])?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: #test.b [b:UInt32]\
        \n  Semi Join: #test.c = #sq.c [a:UInt32, b:UInt32, c:UInt32]\
        \n      TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

        //COUNT(DISTINCT a) AS cd_a

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }
}
