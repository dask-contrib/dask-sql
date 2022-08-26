use crate::sql::rules::type_coercion::TypeCoercion;
use datafusion_common::DataFusionError;
use datafusion_expr::LogicalPlan;
use datafusion_optimizer::decorrelate_scalar_subquery::DecorrelateScalarSubquery;
use datafusion_optimizer::decorrelate_where_exists::DecorrelateWhereExists;
use datafusion_optimizer::decorrelate_where_in::DecorrelateWhereIn;
use datafusion_optimizer::{
    common_subexpr_eliminate::CommonSubexprEliminate, eliminate_limit::EliminateLimit,
    filter_null_join_keys::FilterNullJoinKeys, filter_push_down::FilterPushDown,
    limit_push_down::LimitPushDown, optimizer::OptimizerRule,
    projection_push_down::ProjectionPushDown, subquery_filter_to_join::SubqueryFilterToJoin,
    OptimizerConfig,
};

/// Houses the optimization logic for Dask-SQL. This optimization controls the optimizations
/// and their ordering in regards to their impact on the underlying `LogicalPlan` instance
pub struct DaskSqlOptimizer {
    optimizations: Vec<Box<dyn OptimizerRule + Send + Sync>>,
}

impl DaskSqlOptimizer {
    /// Creates a new instance of the DaskSqlOptimizer with all the DataFusion desired
    /// optimizers as well as any custom `OptimizerRule` trait impls that might be desired.
    pub fn new() -> Self {
        let rules: Vec<Box<dyn OptimizerRule + Send + Sync>> = vec![
            Box::new(CommonSubexprEliminate::new()),
            Box::new(DecorrelateWhereExists::new()),
            Box::new(DecorrelateWhereIn::new()),
            Box::new(DecorrelateScalarSubquery::new()),
            Box::new(EliminateLimit::new()),
            Box::new(FilterNullJoinKeys::default()),
            Box::new(FilterPushDown::new()),
            Box::new(TypeCoercion::new()),
            Box::new(LimitPushDown::new()),
            Box::new(ProjectionPushDown::new()),
            // Box::new(SingleDistinctToGroupBy::new()),
            Box::new(SubqueryFilterToJoin::new()),
        ];
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
            match optimization.optimize(&resulting_plan, &mut OptimizerConfig::new()) {
                Ok(optimized_plan) => resulting_plan = optimized_plan,
                Err(e) => {
                    println!(
                        "Skipping optimizer rule {} due to unexpected error: {}",
                        optimization.name(),
                        e
                    );
                }
            }
        }
        Ok(resulting_plan)
    }
}
