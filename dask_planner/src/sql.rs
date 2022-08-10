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
    sql::exceptions::{py_optimization_exp, py_parsing_exp, py_runtime_err},
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

use sqlparser::dialect::DaskDialect;

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
                            fields.push(Field::new(
                                column_name,
                                DataType::from(column_type.data_type()),
                                true,
                            ));
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
        let fun: ScalarFunctionImplementation =
            Arc::new(|_| Err(DataFusionError::NotImplemented("".to_string())));
        if "year".eq(name) {
            let sig = Signature::variadic(vec![DataType::Int64], Volatility::Immutable);
            let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Int64)));
            return Some(Arc::new(ScalarUDF::new("year", &sig, &rtf, &fun)));
        }
        if "atan2".eq(name) | "mod".eq(name) {
            let sig = Signature::variadic(
                vec![DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            );
            let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Float64)));
            return Some(Arc::new(ScalarUDF::new(name, &sig, &rtf, &fun)));
        }
        if "cbrt".eq(name)
            | "cot".eq(name)
            | "degrees".eq(name)
            | "radians".eq(name)
            | "sign".eq(name)
            | "truncate".eq(name)
        {
            let sig = Signature::variadic(vec![DataType::Float64], Volatility::Immutable);
            let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Float64)));
            return Some(Arc::new(ScalarUDF::new(name, &sig, &rtf, &fun)));
        }

        // Loop through all of the user defined functions
        for (_schema_name, schema) in &self.schemas {
            for (fun_name, function) in &schema.functions {
                if fun_name.eq(name) {
                    let sig = Signature::variadic(vec![DataType::Int64], Volatility::Immutable);
                    let d_type: DataType = function.return_type.clone().into();
                    let rtf: ReturnTypeFunction =
                        Arc::new(|d_type| Ok(Arc::new(d_type[0].clone())));
                    return Some(Arc::new(ScalarUDF::new(
                        fun_name.as_str(),
                        &sig,
                        &rtf,
                        &fun,
                    )));
                }
            }
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

    /// Register a function with the current DaskSQLContext under the specified schema
    pub fn register_function(
        &mut self,
        schema_name: String,
        function: function::DaskFunction,
    ) -> PyResult<bool> {
        match self.schemas.get_mut(&schema_name) {
            Some(schema) => {
                schema.add_function(function);
                Ok(true)
            }
            None => Err(py_runtime_err(format!(
                "Schema: {} not found in DaskSQLContext",
                schema_name
            ))),
        }
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
            None => Err(py_runtime_err(format!(
                "Schema: {} not found in DaskSQLContext",
                schema_name
            ))),
        }
    }

    /// Parses a SQL string into an AST presented as a Vec of Statements
    pub fn parse_sql(&self, sql: &str) -> PyResult<Vec<statement::PyStatement>> {
        let dd: DaskDialect = DaskDialect {};
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
            Err(e) => Err(py_parsing_exp(e)),
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
            .map_err(|e| py_parsing_exp(e))
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
                        .map_err(|e| py_optimization_exp(e))
                } else {
                    // This LogicalPlan does not support Optimization. Return original
                    Ok(existing_plan)
                }
            }
            Err(e) => Err(py_optimization_exp(e)),
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
