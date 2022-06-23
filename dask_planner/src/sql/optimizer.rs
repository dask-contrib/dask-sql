use datafusion_common::DataFusionError;
use datafusion_expr::LogicalPlan;
use datafusion_optimizer::{
    common_subexpr_eliminate::CommonSubexprEliminate, eliminate_limit::EliminateLimit,
    filter_push_down::FilterPushDown, limit_push_down::LimitPushDown, optimizer::OptimizerRule,
    projection_push_down::ProjectionPushDown, single_distinct_to_groupby::SingleDistinctToGroupBy,
    subquery_filter_to_join::SubqueryFilterToJoin, OptimizerConfig,
};

mod filter_null_join_keys;

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
        rules.push(Box::new(EliminateLimit::new()));
        rules.push(Box::new(
            filter_null_join_keys::FilterNullJoinKeys::default(),
        ));
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
            match optimization.optimize(&resulting_plan, &OptimizerConfig::new()) {
                Ok(optimized_plan) => resulting_plan = optimized_plan,
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(resulting_plan)
    }
}
