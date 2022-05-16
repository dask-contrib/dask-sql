pub mod column;
pub mod exceptions;
pub mod function;
pub mod logical;
pub mod schema;
pub mod statement;
pub mod table;
pub mod types;

use crate::sql::exceptions::ParsingException;

use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::catalog::{ResolvedTableReference, TableReference};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::ScalarFunctionImplementation;
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::sql::parser::DFParser;
use datafusion::sql::planner::{ContextProvider, SqlToRel};

use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::table::DaskTableProvider;
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
/// # use datafusion::error::Result;
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
    ) -> Result<Arc<dyn table::TableProvider>, DataFusionError> {
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
                    Some(e) => Ok(Arc::new(DaskTableProvider::new(Arc::new(
                        table::DaskTableSource::new(Arc::new(e)),
                    )))),
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

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        let _f: ScalarFunctionImplementation =
            Arc::new(|_| Err(DataFusionError::NotImplemented("".to_string())));
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        unimplemented!("RUST: get_aggregate_meta is not yet implemented for DaskSQLContext");
    }

    fn get_variable_type(&self, _: &[String]) -> Option<datafusion::arrow::datatypes::DataType> {
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
        match DFParser::parse_sql(sql) {
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
}
