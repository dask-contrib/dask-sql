use datafusion_common::Result;
use datafusion_expr::{logical_plan::LogicalPlan, utils::from_plan};
use datafusion_optimizer::{utils, OptimizerConfig, OptimizerRule};

#[derive(Default)]
pub struct EliminateDoubleDistinct {}

impl EliminateDoubleDistinct {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateDoubleDistinct {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Distinct(distinct) => match distinct.input.as_ref() {
                LogicalPlan::Distinct(extra_distinct) => {
                    let input =
                        utils::optimize_children(self, &extra_distinct.input, optimizer_config)?;
                    let new_plan = from_plan(plan, &plan.expressions(), &[input])?;
                    Ok(new_plan)
                }
                _ => utils::optimize_children(self, plan, optimizer_config),
            },
            _ => utils::optimize_children(self, plan, optimizer_config),
        }
    }

    fn name(&self) -> &str {
        "eliminate_double_distinct"
    }
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

    /// Optimize with just the eliminate_double_distinct rule
    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = EliminateDoubleDistinct::new();
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
    fn test_single_double_distinct() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan("a"))
            .distinct()?
            .distinct()?
            .build()?;

        let expected = "Distinct:\
        \n  TableScan: a";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn test_intersect_double_distinct() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan("a"))
            .distinct()?
            .build()?;

        let right = LogicalPlanBuilder::from(test_table_scan("b"))
            .distinct()?
            .build()?;

        let plan = LogicalPlanBuilder::intersect(left, right, false)?;

        let expected = "LeftSemi Join: a.a = b.a, a.b = b.b, a.c = b.c, a.d = b.d\
        \n  Distinct:\
        \n    TableScan: a\
        \n  Distinct:\
        \n    TableScan: b";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn test_nested_intersect_double_distinct() -> Result<()> {
        let r1 = LogicalPlanBuilder::from(test_table_scan("a"))
            .distinct()?
            .build()?;

        let r2 = LogicalPlanBuilder::from(test_table_scan("b"))
            .distinct()?
            .build()?;

        let l1 = LogicalPlanBuilder::from(test_table_scan("c"))
            .distinct()?
            .build()?;

        let l2 = LogicalPlanBuilder::from(test_table_scan("d"))
            .distinct()?
            .build()?;

        let right = LogicalPlanBuilder::intersect(r1, r2, false)?;

        let left = LogicalPlanBuilder::intersect(l1, l2, false)?;

        let plan = LogicalPlanBuilder::intersect(left, right, false)?;

        let expected = "LeftSemi Join: a.a = b.a, a.b = b.b, a.c = b.c, a.d = b.d\
        \n  Distinct:\
        \n    Tablescan: a\
        \n  Distinct:\
        \n    Tablescan: b";
        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }
}
