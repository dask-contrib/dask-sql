use std::sync::Arc;

use datafusion_common::DataFusionError;
use datafusion_expr::LogicalPlan;
use datafusion_optimizer::{
    common_subexpr_eliminate::CommonSubexprEliminate,
    decorrelate_where_exists::DecorrelateWhereExists,
    decorrelate_where_in::DecorrelateWhereIn,
    eliminate_cross_join::EliminateCrossJoin,
    eliminate_limit::EliminateLimit,
    eliminate_outer_join::EliminateOuterJoin,
    filter_null_join_keys::FilterNullJoinKeys,
    inline_table_scan::InlineTableScan,
    optimizer::{Optimizer, OptimizerRule},
    push_down_filter::PushDownFilter,
    push_down_limit::PushDownLimit,
    push_down_projection::PushDownProjection,
    rewrite_disjunctive_predicate::RewriteDisjunctivePredicate,
    scalar_subquery_to_join::ScalarSubqueryToJoin,
    simplify_expressions::SimplifyExpressions,
    type_coercion::TypeCoercion,
    unwrap_cast_in_comparison::UnwrapCastInComparison,
    OptimizerContext,
};
use log::{debug, trace};

mod filter_columns_post_join;

mod join_reorder;
use join_reorder::JoinReorder;

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
            Arc::new(InlineTableScan::new()),
            Arc::new(TypeCoercion::new()),
            Arc::new(SimplifyExpressions::new()),
            Arc::new(UnwrapCastInComparison::new()),
            Arc::new(DecorrelateWhereExists::new()),
            Arc::new(DecorrelateWhereIn::new()),
            Arc::new(ScalarSubqueryToJoin::new()),
            // simplify expressions does not simplify expressions in subqueries, so we
            // run it again after running the optimizations that potentially converted
            // subqueries to joins
            Arc::new(SimplifyExpressions::new()),
            // TODO: need to handle EmptyRelation for GPU cases
            // Arc::new(EliminateFilter::new()),
            Arc::new(EliminateCrossJoin::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(EliminateLimit::new()),
            Arc::new(RewriteDisjunctivePredicate::new()),
            Arc::new(FilterNullJoinKeys::default()),
            Arc::new(EliminateOuterJoin::new()),
            Arc::new(PushDownFilter::new()),
            Arc::new(PushDownLimit::new()),
            // Dask-SQL specific optimizations
            // Arc::new(FilterColumnsPostJoin::new()),
            Arc::new(JoinReorder::default()),
            // The previous optimizations added expressions and projections,
            // that might benefit from the following rules
            Arc::new(SimplifyExpressions::new()),
            Arc::new(UnwrapCastInComparison::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(PushDownProjection::new()),
        ];

        Self {
            optimizer: Optimizer::with_rules(rules),
        }
    }

    /// Iterates through the configured `OptimizerRule`(s) to transform the input `LogicalPlan`
    /// to its final optimized form
    pub(crate) fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
        let config = OptimizerContext::new();
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

    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::{config::ConfigOptions, DataFusionError, Result};
    use datafusion_expr::{AggregateUDF, LogicalPlan, ScalarUDF, TableSource};
    use datafusion_sql::{
        planner::{ContextProvider, SqlToRel},
        sqlparser::{ast::Statement, parser::Parser},
        TableReference,
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
        let expected = r#"Projection: test.col_int32
  Filter: CAST(test.col_int32 AS Float64) > __scalar_sq_1.__value
    CrossJoin:
      TableScan: test projection=[col_int32]
      SubqueryAlias: __scalar_sq_1
        Projection: AVG(test.col_int32) AS __value
          Aggregate: groupBy=[[]], aggr=[[AVG(test.col_int32)]]
            Filter: test.col_utf8 >= Utf8("2002-05-08") AND test.col_utf8 <= Utf8("2002-05-13")
              TableScan: test projection=[col_int32, col_utf8]"#;
        assert_eq!(expected, format!("{:?}", plan));
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
        ) -> datafusion_common::Result<Arc<dyn TableSource>> {
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
