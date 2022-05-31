use datafusion::error::DataFusionError;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::eliminate_filter::EliminateFilter;
use datafusion::optimizer::eliminate_limit::EliminateLimit;
use datafusion::optimizer::filter_push_down::FilterPushDown;
use datafusion::optimizer::limit_push_down::LimitPushDown;
use datafusion::optimizer::optimizer::OptimizerRule;

use datafusion::optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use datafusion::optimizer::projection_push_down::ProjectionPushDown;
use datafusion::optimizer::single_distinct_to_groupby::SingleDistinctToGroupBy;
use datafusion::optimizer::subquery_filter_to_join::SubqueryFilterToJoin;

/// Houses the optimization logic for Dask-SQL. This optimization controls the optimizations
/// and their ordering in regards to their impact on the underlying `LogicalPlan` instance
pub struct DaskSqlOptimizer {
    optimizations: Vec<Box<dyn OptimizerRule + Send + Sync>>,
}

impl DaskSqlOptimizer {
    /// Creates a new instance of the DaskSqlOptimizer with all the DataFusion desired
    /// optimizers as well as any custom `OptimizerRule` trait impls that might be desired.
    pub fn new() -> Self {
        let mut rules: Vec<Box<dyn OptimizerRule + Send + Sync>> = Vec::new();
        rules.push(Box::new(CommonSubexprEliminate::new()));
        rules.push(Box::new(EliminateFilter::new()));
        rules.push(Box::new(EliminateLimit::new()));
        rules.push(Box::new(FilterPushDown::new()));
        rules.push(Box::new(LimitPushDown::new()));
        rules.push(Box::new(ProjectionPushDown::new()));
        rules.push(Box::new(SingleDistinctToGroupBy::new()));
        rules.push(Box::new(SubqueryFilterToJoin::new()));
        Self {
            optimizations: rules,
        }
    }

    /// Iteratoes through the configured `OptimizerRule`(s) to transform the input `LogicalPlan`
    /// to its final optimized form
    pub(crate) fn run_optimizations(
        &self,
        plan: LogicalPlan,
    ) -> Result<LogicalPlan, DataFusionError> {
        let mut resulting_plan: LogicalPlan = plan;
        for optimization in &self.optimizations {
            match optimization.optimize(&resulting_plan, &ExecutionProps::new()) {
                Ok(optimized_plan) => resulting_plan = optimized_plan,
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(resulting_plan)
    }
}
