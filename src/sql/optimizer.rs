// Declare optimizer modules
pub mod decorrelate_where_exists;
pub mod decorrelate_where_in;
pub mod dynamic_partition_pruning;
pub mod join_reorder;
pub mod utils;

use std::sync::Arc;

use datafusion_python::{
    datafusion_common::DataFusionError,
    datafusion_expr::LogicalPlan,
    datafusion_optimizer::{
        eliminate_cross_join::EliminateCrossJoin,
        eliminate_limit::EliminateLimit,
        eliminate_outer_join::EliminateOuterJoin,
        eliminate_project::EliminateProjection,
        filter_null_join_keys::FilterNullJoinKeys,
        optimizer::{Optimizer, OptimizerRule},
        push_down_filter::PushDownFilter,
        push_down_limit::PushDownLimit,
        push_down_projection::PushDownProjection,
        rewrite_disjunctive_predicate::RewriteDisjunctivePredicate,
        scalar_subquery_to_join::ScalarSubqueryToJoin,
        simplify_expressions::SimplifyExpressions,
        unwrap_cast_in_comparison::UnwrapCastInComparison,
        OptimizerContext,
    },
};
use decorrelate_where_exists::DecorrelateWhereExists;
use decorrelate_where_in::DecorrelateWhereIn;
use dynamic_partition_pruning::DynamicPartitionPruning;
use join_reorder::JoinReorder;
use log::{debug, trace};

/// Houses the optimization logic for Dask-SQL. This optimization controls the optimizations
/// and their ordering in regards to their impact on the underlying `LogicalPlan` instance
pub struct DaskSqlOptimizer {
    optimizer: Optimizer,
}

impl DaskSqlOptimizer {
    /// Creates a new instance of the DaskSqlOptimizer with all the DataFusion desired
    /// optimizers as well as any custom `OptimizerRule` trait impls that might be desired.
    pub fn new() -> Self {
        debug!("Creating new instance of DaskSqlOptimizer");

        let rules: Vec<Arc<dyn OptimizerRule + Sync + Send>> = vec![
            Arc::new(SimplifyExpressions::new()),
            Arc::new(UnwrapCastInComparison::new()),
            // Arc::new(ReplaceDistinctWithAggregate::new()),
            Arc::new(DecorrelateWhereExists::new()),
            Arc::new(DecorrelateWhereIn::new()),
            Arc::new(ScalarSubqueryToJoin::new()),
            //Arc::new(ExtractEquijoinPredicate::new()),

            // simplify expressions does not simplify expressions in subqueries, so we
            // run it again after running the optimizations that potentially converted
            // subqueries to joins
            Arc::new(SimplifyExpressions::new()),
            // Arc::new(MergeProjection::new()),
            Arc::new(RewriteDisjunctivePredicate::new()),
            // Arc::new(EliminateDuplicatedExpr::new()),

            // TODO: need to handle EmptyRelation for GPU cases
            // Arc::new(EliminateFilter::new()),
            Arc::new(EliminateCrossJoin::new()),
            // Arc::new(CommonSubexprEliminate::new()),
            Arc::new(EliminateLimit::new()),
            // Arc::new(PropagateEmptyRelation::new()),
            Arc::new(FilterNullJoinKeys::default()),
            Arc::new(EliminateOuterJoin::new()),
            // Filters can't be pushed down past Limits, we should do PushDownFilter after PushDownLimit
            Arc::new(PushDownLimit::new()),
            Arc::new(PushDownFilter::new()),
            // Arc::new(SingleDistinctToGroupBy::new()),
            // Dask-SQL specific optimizations
            Arc::new(JoinReorder::default()),
            // The previous optimizations added expressions and projections,
            // that might benefit from the following rules
            Arc::new(SimplifyExpressions::new()),
            Arc::new(UnwrapCastInComparison::new()),
            // Arc::new(CommonSubexprEliminate::new()),
            Arc::new(PushDownProjection::new()),
            Arc::new(EliminateProjection::new()),
            // PushDownProjection can pushdown Projections through Limits, do PushDownLimit again.
            Arc::new(PushDownLimit::new()),
        ];

        Self {
            optimizer: Optimizer::with_rules(rules),
        }
    }

    // Create a separate instance of this optimization rule, since we want to ensure that it only
    // runs one time
    pub fn dynamic_partition_pruner() -> Self {
        let rule: Vec<Arc<dyn OptimizerRule + Sync + Send>> =
            vec![Arc::new(DynamicPartitionPruning::new())];

        Self {
            optimizer: Optimizer::with_rules(rule),
        }
    }

    /// Iterates through the configured `OptimizerRule`(s) to transform the input `LogicalPlan`
    /// to its final optimized form
    pub(crate) fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
        let config = OptimizerContext::new();
        self.optimizer.optimize(&plan, &config, Self::observe)
    }

    /// Iterates once through the configured `OptimizerRule`(s) to transform the input `LogicalPlan`
    /// to its final optimized form
    pub(crate) fn optimize_once(&self, plan: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
        let mut config = OptimizerContext::new();
        config = OptimizerContext::with_max_passes(config, 1);
        self.optimizer.optimize(&plan, &config, Self::observe)
    }

    fn observe(optimized_plan: &LogicalPlan, optimization: &dyn OptimizerRule) {
        trace!(
            "== AFTER APPLYING RULE {} ==\n{}\n",
            optimization.name(),
            optimized_plan.display_indent()
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{any::Any, collections::HashMap, sync::Arc};

    use datafusion_python::{
        datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef},
        datafusion_common::{config::ConfigOptions, DataFusionError, Result},
        datafusion_expr::{AggregateUDF, LogicalPlan, ScalarUDF, TableSource},
        datafusion_sql::{
            planner::{ContextProvider, SqlToRel},
            sqlparser::{ast::Statement, parser::Parser},
            TableReference,
        },
    };

    use crate::{dialect::DaskDialect, sql::optimizer::DaskSqlOptimizer};

    #[test]
    fn subquery_filter_with_cast() -> Result<()> {
        // regression test for https://github.com/apache/arrow-datafusion/issues/3760
        let sql = "SELECT col_int32 FROM test \
    WHERE col_int32 > (\
      SELECT AVG(col_int32) FROM test \
      WHERE col_utf8 BETWEEN '2002-05-08' \
        AND (cast('2002-05-08' as date) + interval '5 days')\
    )";
        let plan = test_sql(sql)?;
        assert!(format!("{:?}", plan).contains(r#"<= Date32("11820")"#));
        Ok(())
    }

    fn test_sql(sql: &str) -> Result<LogicalPlan> {
        // parse the SQL
        let dialect = DaskDialect {};
        let ast: Vec<Statement> = Parser::parse_sql(&dialect, sql).unwrap();
        let statement = &ast[0];

        // create a logical query plan
        let schema_provider = MySchemaProvider::new();
        let sql_to_rel = SqlToRel::new(&schema_provider);
        let plan = sql_to_rel.sql_statement_to_plan(statement.clone()).unwrap();

        // optimize the logical plan
        let optimizer = DaskSqlOptimizer::new();
        optimizer.optimize(plan)
    }

    struct MySchemaProvider {
        options: ConfigOptions,
    }

    impl MySchemaProvider {
        fn new() -> Self {
            Self {
                options: ConfigOptions::default(),
            }
        }
    }

    impl ContextProvider for MySchemaProvider {
        fn options(&self) -> &ConfigOptions {
            &self.options
        }

        fn get_table_provider(
            &self,
            name: TableReference,
        ) -> datafusion_python::datafusion_common::Result<Arc<dyn TableSource>> {
            let table_name = name.table();
            if table_name.starts_with("test") {
                let schema = Schema::new_with_metadata(
                    vec![
                        Field::new("col_int32", DataType::Int32, true),
                        Field::new("col_uint32", DataType::UInt32, true),
                        Field::new("col_utf8", DataType::Utf8, true),
                        Field::new("col_date32", DataType::Date32, true),
                        Field::new("col_date64", DataType::Date64, true),
                    ],
                    HashMap::new(),
                );

                Ok(Arc::new(MyTableSource {
                    schema: Arc::new(schema),
                }))
            } else {
                Err(DataFusionError::Plan("table does not exist".to_string()))
            }
        }

        fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
            None
        }

        fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
            None
        }

        fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
            None
        }

        fn get_window_meta(
            &self,
            _name: &str,
        ) -> Option<Arc<datafusion_python::datafusion_expr::WindowUDF>> {
            None
        }
    }

    struct MyTableSource {
        schema: SchemaRef,
    }

    impl TableSource for MyTableSource {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }
}
