pub mod column;
pub mod exceptions;
pub mod function;
pub mod logical;
pub mod optimizer;
pub mod parser_utils;
pub mod rules;
pub mod schema;
pub mod statement;
pub mod table;
pub mod types;

use crate::sql::exceptions::{py_optimization_exp, py_parsing_exp, py_runtime_err};

use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::{DFSchema, DataFusionError};
use datafusion_expr::logical_plan::Extension;
use datafusion_expr::{
    AggregateUDF, LogicalPlan, PlanVisitor, ReturnTypeFunction, ScalarFunctionImplementation,
    ScalarUDF, Signature, TableSource, TypeSignature, Volatility,
};
use datafusion_sql::{
    parser::Statement as DFStatement,
    planner::{ContextProvider, SqlToRel},
    ResolvedTableReference, TableReference,
};

use std::collections::HashMap;
use std::sync::Arc;

use crate::dialect::DaskDialect;
use crate::parser::{DaskParser, DaskStatement};
use crate::sql::logical::create_model::CreateModelPlanNode;
use crate::sql::logical::create_table::CreateTablePlanNode;
use crate::sql::logical::drop_model::DropModelPlanNode;
use crate::sql::logical::predict_model::PredictModelPlanNode;
use crate::sql::logical::show_columns::ShowColumnsPlanNode;
use crate::sql::logical::show_schema::ShowSchemasPlanNode;
use crate::sql::logical::show_tables::ShowTablesPlanNode;

use crate::sql::logical::PyLogicalPlan;
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
                for table in schema.tables.values() {
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

        match name {
            "year" => {
                let sig = Signature::variadic(vec![DataType::Int64], Volatility::Immutable);
                let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Int64)));
                return Some(Arc::new(ScalarUDF::new(name, &sig, &rtf, &fun)));
            }
            "atan2" | "mod" => {
                let sig = Signature::variadic(
                    vec![DataType::Float64, DataType::Float64],
                    Volatility::Immutable,
                );
                let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Float64)));
                return Some(Arc::new(ScalarUDF::new(name, &sig, &rtf, &fun)));
            }
            "cbrt" | "cot" | "degrees" | "radians" | "sign" | "truncate" => {
                let sig = Signature::variadic(vec![DataType::Float64], Volatility::Immutable);
                let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Float64)));
                return Some(Arc::new(ScalarUDF::new(name, &sig, &rtf, &fun)));
            }
            "rand" => {
                let sig = Signature::variadic(vec![DataType::Int64], Volatility::Volatile);
                let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Float64)));
                return Some(Arc::new(ScalarUDF::new(name, &sig, &rtf, &fun)));
            }
            "rand_integer" => {
                let sig = Signature::variadic(
                    vec![DataType::Int64, DataType::Int64],
                    Volatility::Volatile,
                );
                let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Int64)));
                return Some(Arc::new(ScalarUDF::new(name, &sig, &rtf, &fun)));
            }
            _ => (),
        }

        // Loop through all of the user defined functions
        for schema in self.schemas.values() {
            for (fun_name, function) in &schema.functions {
                if fun_name.eq(name) {
                    let sig = {
                        let function = function.lock().unwrap();
                        Signature::one_of(
                            function
                                .return_types
                                .keys()
                                .map(|v| TypeSignature::Exact(v.to_vec()))
                                .collect(),
                            Volatility::Immutable,
                        )
                    };
                    let function = function.clone();
                    let rtf: ReturnTypeFunction = Arc::new(move |input_types| {
                        let function = function.lock().unwrap();
                        match function.return_types.get(&input_types.to_vec()) {
                            Some(return_type) => Ok(Arc::new(return_type.clone())),
                            None => Err(DataFusionError::Plan(format!(
                                "UDF signature not found for input types {:?}",
                                input_types
                            ))),
                        }
                    });
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
    pub fn parse_sql(&self, sql: String) -> PyResult<Vec<statement::PyStatement>> {
        let dd: DaskDialect = DaskDialect {};
        match DaskParser::parse_sql_with_dialect(sql.as_str(), &dd) {
            Ok(k) => {
                let mut statements: Vec<statement::PyStatement> = Vec::new();
                for statement in k {
                    statements.push(statement.into());
                }
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
        self._logical_relational_algebra(statement.statement)
            .map(|e| PyLogicalPlan {
                original_plan: e,
                current_node: None,
            })
            .map_err(py_parsing_exp)
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
                        .map(|k| PyLogicalPlan {
                            original_plan: k,
                            current_node: None,
                        })
                        .map_err(py_optimization_exp)
                } else {
                    // This LogicalPlan does not support Optimization. Return original
                    Ok(existing_plan)
                }
            }
            Err(e) => Err(py_optimization_exp(e)),
        }
    }
}

/// non-Python methods
impl DaskSQLContext {
    /// Creates a non-optimized Relational Algebra LogicalPlan from an AST Statement
    pub fn _logical_relational_algebra(
        &self,
        dask_statement: DaskStatement,
    ) -> Result<LogicalPlan, DataFusionError> {
        match dask_statement {
            DaskStatement::Statement(statement) => {
                let planner = SqlToRel::new(self);
                planner.statement_to_plan(DFStatement::Statement(statement))
            }
            DaskStatement::CreateModel(create_model) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CreateModelPlanNode {
                    model_name: create_model.name,
                    input: self._logical_relational_algebra(DaskStatement::Statement(Box::new(
                        create_model.select,
                    )))?,
                    if_not_exists: create_model.if_not_exists,
                    or_replace: create_model.or_replace,
                    with_options: create_model.with_options,
                }),
            })),
            DaskStatement::PredictModel(predict_model) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(PredictModelPlanNode {
                    model_schema: predict_model.schema_name,
                    model_name: predict_model.name,
                    input: self._logical_relational_algebra(DaskStatement::Statement(Box::new(
                        predict_model.select,
                    )))?,
                }),
            })),
            DaskStatement::CreateTable(create_table) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CreateTablePlanNode {
                    schema: Arc::new(DFSchema::empty()),
                    table_schema: create_table.table_schema,
                    table_name: create_table.name,
                    if_not_exists: create_table.if_not_exists,
                    or_replace: create_table.or_replace,
                    with_options: create_table.with_options,
                }),
            })),
            DaskStatement::DropModel(drop_model) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(DropModelPlanNode {
                    model_name: drop_model.name,
                    if_exists: drop_model.if_exists,
                    schema: Arc::new(DFSchema::empty()),
                }),
            })),
            DaskStatement::ShowSchemas(show_schemas) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(ShowSchemasPlanNode {
                    schema: Arc::new(DFSchema::empty()),
                    like: show_schemas.like,
                }),
            })),
            DaskStatement::ShowTables(show_tables) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(ShowTablesPlanNode {
                    schema: Arc::new(DFSchema::empty()),
                    schema_name: show_tables.schema_name,
                }),
            })),
            DaskStatement::ShowColumns(show_columns) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(ShowColumnsPlanNode {
                    schema: Arc::new(DFSchema::empty()),
                    table_name: show_columns.table_name,
                    schema_name: show_columns.schema_name,
                }),
            })),
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
