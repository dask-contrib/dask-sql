pub mod column;
pub mod exceptions;
pub mod function;
pub mod logical;
pub mod optimizer;
pub mod schema;
pub mod statement;
pub mod table;
pub mod types;

use crate::{
    dialect::DaskSqlDialect,
    sql::exceptions::{OptimizationException, ParsingException},
};

use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::DataFusionError;
use datafusion_expr::{
    AggregateUDF, LogicalPlan, PlanVisitor, ReturnTypeFunction, ScalarFunctionImplementation,
    ScalarUDF, Signature, TableSource, Volatility,
};
use datafusion_sql::{
    parser::DFParser,
    planner::{ContextProvider, SqlToRel},
    ResolvedTableReference, TableReference,
};

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;

/// DaskSQLContext is main interface used for interacting with DataFusion to
/// parse SQL queries, build logical plans, and optimize logical plans.
///
/// The following example demonstrates how to generate an optimized LogicalPlan
/// from SQL using DaskSQLContext.
///
/// ```
/// use datafusion::prelude::*;
///
/// # use datafusion_common::Result;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let mut ctx = DaskSQLContext::new();
/// let parsed_sql = ctx.parse_sql("SELECT COUNT(*) FROM test_table");
/// let nonOptimizedRelAlgebra = ctx.logical_relational_algebra(parsed_sql);
/// let optmizedRelAlg = ctx.optimizeRelationalAlgebra(nonOptimizedRelAlgebra);
/// # Ok(())
/// # }
/// ```
#[pyclass(name = "DaskSQLContext", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct DaskSQLContext {
    default_catalog_name: String,
    default_schema_name: String,
    schemas: HashMap<String, schema::DaskSchema>,
}

impl ContextProvider for DaskSQLContext {
    fn get_table_provider(
        &self,
        name: TableReference,
    ) -> Result<Arc<dyn TableSource>, DataFusionError> {
        let reference: ResolvedTableReference =
            name.resolve(&self.default_catalog_name, &self.default_schema_name);
        match self.schemas.get(&self.default_schema_name) {
            Some(schema) => {
                let mut resp = None;
                for (_table_name, table) in &schema.tables {
                    if table.name.eq(&name.table()) {
                        // Build the Schema here
                        let mut fields: Vec<Field> = Vec::new();
                        // Iterate through the DaskTable instance and create a Schema instance
                        for (column_name, column_type) in &table.columns {
                            fields.push(Field::new(column_name, column_type.data_type(), false));
                        }

                        resp = Some(Schema::new(fields));
                    }
                }

                // If the Table is not found return None. DataFusion will handle the error propagation
                match resp {
                    Some(e) => Ok(Arc::new(table::DaskTableSource::new(Arc::new(e)))),
                    None => Err(DataFusionError::Plan(format!(
                        "Table '{}.{}.{}' not found",
                        reference.catalog, reference.schema, reference.table
                    ))),
                }
            }
            None => Err(DataFusionError::Plan(format!(
                "Unable to locate Schema: '{}.{}'",
                reference.catalog, reference.schema
            ))),
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        let f: ScalarFunctionImplementation = Arc::new(|_| {
            Err(DataFusionError::NotImplemented(
                "Year function implementation".to_string(),
            ))
        });
        if "year".eq(name) {
            let sig = Signature::variadic(vec![DataType::Int64], Volatility::Immutable);
            let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Int64)));
            return Some(Arc::new(ScalarUDF::new("year", &sig, &rtf, &f)));
        }
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, _: &[String]) -> Option<arrow::datatypes::DataType> {
        unimplemented!("RUST: get_variable_type is not yet implemented for DaskSQLContext")
    }
}

#[pymethods]
impl DaskSQLContext {
    #[new]
    pub fn new(default_catalog_name: String, default_schema_name: String) -> Self {
        Self {
            default_catalog_name,
            default_schema_name,
            schemas: HashMap::new(),
        }
    }

    /// Register a Schema with the current DaskSQLContext
    pub fn register_schema(
        &mut self,
        schema_name: String,
        schema: schema::DaskSchema,
    ) -> PyResult<bool> {
        self.schemas.insert(schema_name, schema);
        Ok(true)
    }

    /// Register a DaskTable instance under the specified schema in the current DaskSQLContext
    pub fn register_table(
        &mut self,
        schema_name: String,
        table: table::DaskTable,
    ) -> PyResult<bool> {
        match self.schemas.get_mut(&schema_name) {
            Some(schema) => {
                schema.add_table(table);
                Ok(true)
            }
            None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Schema: {} not found in DaskSQLContext",
                schema_name
            ))),
        }
    }

    /// Parses a SQL string into an AST presented as a Vec of Statements
    pub fn parse_sql(&self, sql: &str) -> PyResult<Vec<statement::PyStatement>> {
        let dd: DaskSqlDialect = DaskSqlDialect {};
        match DFParser::parse_sql_with_dialect(sql, &dd) {
            Ok(k) => {
                let mut statements: Vec<statement::PyStatement> = Vec::new();
                for statement in k {
                    statements.push(statement.into());
                }
                assert!(
                    statements.len() == 1,
                    "More than 1 expected statement was encounterd!"
                );
                Ok(statements)
            }
            Err(e) => Err(PyErr::new::<ParsingException, _>(format!("{}", e))),
        }
    }

    /// Creates a non-optimized Relational Algebra LogicalPlan from an AST Statement
    pub fn logical_relational_algebra(
        &self,
        statement: statement::PyStatement,
    ) -> PyResult<logical::PyLogicalPlan> {
        let planner = SqlToRel::new(self);
        planner
            .statement_to_plan(statement.statement)
            .map(|k| logical::PyLogicalPlan {
                original_plan: k,
                current_node: None,
            })
            .map_err(|e| PyErr::new::<ParsingException, _>(format!("{}", e)))
    }

    /// Accepts an existing relational plan, `LogicalPlan`, and optimizes it
    /// by applying a set of `optimizer` trait implementations against the
    /// `LogicalPlan`
    pub fn optimize_relational_algebra(
        &self,
        existing_plan: logical::PyLogicalPlan,
    ) -> PyResult<logical::PyLogicalPlan> {
        // Certain queries cannot be optimized. Ex: `EXPLAIN SELECT * FROM test` simply return those plans as is
        let mut visitor = OptimizablePlanVisitor {};

        match existing_plan.original_plan.accept(&mut visitor) {
            Ok(valid) => {
                if valid {
                    optimizer::DaskSqlOptimizer::new()
                        .run_optimizations(existing_plan.original_plan)
                        .map(|k| logical::PyLogicalPlan {
                            original_plan: k,
                            current_node: None,
                        })
                        .map_err(|e| PyErr::new::<OptimizationException, _>(format!("{}", e)))
                } else {
                    // This LogicalPlan does not support Optimization. Return original
                    Ok(existing_plan)
                }
            }
            Err(e) => Err(PyErr::new::<OptimizationException, _>(format!("{}", e))),
        }
    }
}

/// Visits each AST node to determine if the plan is valid for optimization or not
pub struct OptimizablePlanVisitor;

impl PlanVisitor for OptimizablePlanVisitor {
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &LogicalPlan) -> std::result::Result<bool, DataFusionError> {
        // If the plan contains an unsupported Node type we flag the plan as un-optimizable here
        match plan {
            LogicalPlan::Explain(..) => Ok(false),
            _ => Ok(true),
        }
    }

    fn post_visit(&mut self, _plan: &LogicalPlan) -> std::result::Result<bool, DataFusionError> {
        Ok(true)
    }
}
