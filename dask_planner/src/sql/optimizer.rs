use datafusion_common::DataFusionError;
use datafusion_expr::LogicalPlan;
use datafusion_optimizer::decorrelate_where_exists::DecorrelateWhereExists;
use datafusion_optimizer::decorrelate_where_in::DecorrelateWhereIn;
use datafusion_optimizer::eliminate_filter::EliminateFilter;
use datafusion_optimizer::pre_cast_lit_in_comparison::PreCastLitInComparisonExpressions;
use datafusion_optimizer::reduce_cross_join::ReduceCrossJoin;
use datafusion_optimizer::reduce_outer_join::ReduceOuterJoin;
use datafusion_optimizer::rewrite_disjunctive_predicate::RewriteDisjunctivePredicate;
use datafusion_optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;
use datafusion_optimizer::simplify_expressions::SimplifyExpressions;
use datafusion_optimizer::type_coercion::TypeCoercion;
use datafusion_optimizer::{
    common_subexpr_eliminate::CommonSubexprEliminate, eliminate_limit::EliminateLimit,
    filter_null_join_keys::FilterNullJoinKeys, filter_push_down::FilterPushDown,
    limit_push_down::LimitPushDown, optimizer::OptimizerRule,
    projection_push_down::ProjectionPushDown, subquery_filter_to_join::SubqueryFilterToJoin,
    OptimizerConfig,
};
use log::trace;

mod eliminate_agg_distinct;
use eliminate_agg_distinct::EliminateAggDistinct;

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
            Box::new(PreCastLitInComparisonExpressions::new()),
            Box::new(TypeCoercion::new()),
            Box::new(SimplifyExpressions::new()),
            Box::new(DecorrelateWhereExists::new()),
            Box::new(DecorrelateWhereIn::new()),
            Box::new(ScalarSubqueryToJoin::new()),
            Box::new(SubqueryFilterToJoin::new()),
            Box::new(EliminateFilter::new()),
            Box::new(ReduceCrossJoin::new()),
            Box::new(CommonSubexprEliminate::new()),
            Box::new(EliminateLimit::new()),
            Box::new(ProjectionPushDown::new()),
            Box::new(RewriteDisjunctivePredicate::new()),
            Box::new(FilterNullJoinKeys::default()),
            Box::new(ReduceOuterJoin::new()),
            Box::new(FilterPushDown::new()),
            Box::new(LimitPushDown::new()),
            // Box::new(SingleDistinctToGroupBy::new()),
            // Dask-SQL specific optimizations
            Box::new(EliminateAggDistinct::new()),
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
                Ok(optimized_plan) => {
                    trace!(
                        "== AFTER APPLYING RULE {} ==\n{}",
                        optimization.name(),
                        optimized_plan.display_indent()
                    );
                    resulting_plan = optimized_plan
                }
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
