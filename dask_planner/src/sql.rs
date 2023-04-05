pub mod column;
pub mod exceptions;
pub mod function;
pub mod logical;
pub mod optimizer;
pub mod parser_utils;
pub mod schema;
pub mod statement;
pub mod table;
pub mod types;

use std::{collections::HashMap, sync::Arc};

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion_common::{config::ConfigOptions, DFSchema, DataFusionError};
use datafusion_expr::{
    logical_plan::Extension,
    AccumulatorFunctionImplementation,
    AggregateUDF,
    LogicalPlan,
    PlanVisitor,
    ReturnTypeFunction,
    ScalarFunctionImplementation,
    ScalarUDF,
    Signature,
    StateTypeFunction,
    TableSource,
    TypeSignature,
    Volatility,
};
use datafusion_sql::{
    parser::Statement as DFStatement,
    planner::{ContextProvider, SqlToRel},
    ResolvedTableReference,
    TableReference,
};
use log::{debug, warn};
use pyo3::prelude::*;

use self::logical::{
    create_catalog_schema::CreateCatalogSchemaPlanNode,
    drop_schema::DropSchemaPlanNode,
    use_schema::UseSchemaPlanNode,
};
use crate::{
    dialect::DaskDialect,
    parser::{DaskParser, DaskStatement},
    sql::{
        exceptions::{py_optimization_exp, py_parsing_exp, py_runtime_err},
        logical::{
            alter_schema::AlterSchemaPlanNode,
            alter_table::AlterTablePlanNode,
            analyze_table::AnalyzeTablePlanNode,
            create_experiment::CreateExperimentPlanNode,
            create_model::CreateModelPlanNode,
            create_table::CreateTablePlanNode,
            describe_model::DescribeModelPlanNode,
            drop_model::DropModelPlanNode,
            export_model::ExportModelPlanNode,
            predict_model::PredictModelPlanNode,
            show_columns::ShowColumnsPlanNode,
            show_models::ShowModelsPlanNode,
            show_schemas::ShowSchemasPlanNode,
            show_tables::ShowTablesPlanNode,
            PyLogicalPlan,
        },
    },
};

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
    current_catalog: String,
    current_schema: String,
    schemas: HashMap<String, schema::DaskSchema>,
    options: ConfigOptions,
}

impl ContextProvider for DaskSQLContext {
    fn get_table_provider(
        &self,
        name: TableReference,
    ) -> Result<Arc<dyn TableSource>, DataFusionError> {
        let reference: ResolvedTableReference = name
            .clone()
            .resolve(&self.current_catalog, &self.current_schema);
        if reference.catalog != self.current_catalog {
            // there is a single catalog in Dask SQL
            return Err(DataFusionError::Plan(format!(
                "Cannot resolve catalog '{}'",
                reference.catalog
            )));
        }
        let schema_name = reference.clone().schema.into_owned();
        match self.schemas.get(&schema_name) {
            Some(schema) => {
                let mut resp = None;
                for table in schema.tables.values() {
                    if table.table_name.eq(&name.table()) {
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
                    Some(e) => {
                        let table_ref = &self
                            .schemas
                            .get(reference.schema.as_ref())
                            .unwrap()
                            .tables
                            .get(reference.table.as_ref())
                            .unwrap();
                        let statistics = &table_ref.statistics;
                        let filepath = &table_ref.filepath;
                        if statistics.get_row_count() == 0.0 {
                            Ok(Arc::new(table::DaskTableSource::new(
                                Arc::new(e),
                                None,
                                filepath.clone(),
                            )))
                        } else {
                            Ok(Arc::new(table::DaskTableSource::new(
                                Arc::new(e),
                                Some(statistics.clone()),
                                filepath.clone(),
                            )))
                        }
                    }
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

        let numeric_datatypes = vec![
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
        ];

        match name {
            "year" => {
                let sig = generate_signatures(vec![numeric_datatypes]);
                let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Int64)));
                return Some(Arc::new(ScalarUDF::new(name, &sig, &rtf, &fun)));
            }
            "last_day" => {
                let sig = Signature::exact(
                    vec![DataType::Timestamp(TimeUnit::Nanosecond, None)],
                    Volatility::Immutable,
                );
                let rtf: ReturnTypeFunction =
                    Arc::new(|_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Nanosecond, None))));
                return Some(Arc::new(ScalarUDF::new(name, &sig, &rtf, &fun)));
            }
            "timestampceil" | "timestampfloor" => {
                let sig = Signature::exact(
                    vec![DataType::Date64, DataType::Utf8],
                    Volatility::Immutable,
                );
                let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Date64)));
                return Some(Arc::new(ScalarUDF::new(name, &sig, &rtf, &fun)));
            }
            "timestampadd" => {
                let sig = Signature::one_of(
                    vec![
                        TypeSignature::Exact(vec![
                            DataType::Utf8,
                            DataType::Int64,
                            DataType::Date64,
                        ]),
                        TypeSignature::Exact(vec![
                            DataType::Utf8,
                            DataType::Int64,
                            DataType::Timestamp(TimeUnit::Nanosecond, None),
                        ]),
                    ],
                    Volatility::Immutable,
                );
                let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Date64)));
                return Some(Arc::new(ScalarUDF::new(name, &sig, &rtf, &fun)));
            }
            "timestampdiff" => {
                let sig = Signature::exact(
                    vec![
                        DataType::Utf8,
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                    ],
                    Volatility::Immutable,
                );
                let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Int64)));
                return Some(Arc::new(ScalarUDF::new(name, &sig, &rtf, &fun)));
            }
            "dsql_totimestamp" => {
                let first_datatypes = vec![
                    DataType::Int8,
                    DataType::Int16,
                    DataType::Int32,
                    DataType::Int64,
                    DataType::UInt8,
                    DataType::UInt16,
                    DataType::UInt32,
                    DataType::UInt64,
                    DataType::Utf8,
                ];
                let sig = generate_signatures(vec![first_datatypes, vec![DataType::Utf8]]);
                let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Date64)));
                return Some(Arc::new(ScalarUDF::new(name, &sig, &rtf, &fun)));
            }
            "mod" => {
                let sig = generate_signatures(vec![numeric_datatypes.clone(), numeric_datatypes]);
                let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Float64)));
                return Some(Arc::new(ScalarUDF::new(name, &sig, &rtf, &fun)));
            }
            "cbrt" | "cot" | "degrees" | "radians" | "sign" | "truncate" => {
                let sig = generate_signatures(vec![numeric_datatypes]);
                let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Float64)));
                return Some(Arc::new(ScalarUDF::new(name, &sig, &rtf, &fun)));
            }
            "rand" => {
                let sig = Signature::one_of(
                    vec![
                        TypeSignature::Exact(vec![]),
                        TypeSignature::Exact(vec![DataType::Int64]),
                    ],
                    Volatility::Immutable,
                );
                let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Float64)));
                return Some(Arc::new(ScalarUDF::new(name, &sig, &rtf, &fun)));
            }
            "rand_integer" => {
                let sig = Signature::one_of(
                    vec![
                        TypeSignature::Exact(vec![DataType::Int64]),
                        TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                    ],
                    Volatility::Immutable,
                );
                let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Int64)));
                return Some(Arc::new(ScalarUDF::new(name, &sig, &rtf, &fun)));
            }
            _ => (),
        }

        // Loop through all of the user defined functions
        for schema in self.schemas.values() {
            for (fun_name, func_mutex) in &schema.functions {
                if fun_name.eq(name) {
                    let function = func_mutex.lock().unwrap();
                    if function.aggregation.eq(&true) {
                        return None;
                    }
                    let sig = {
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
                        match function.return_types.get(&input_types.to_vec()) {
                            Some(return_type) => Ok(Arc::new(return_type.clone())),
                            None => Err(DataFusionError::Plan(format!(
                                "UDF signature not found for input types {input_types:?}"
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

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        let acc: AccumulatorFunctionImplementation =
            Arc::new(|_return_type| Err(DataFusionError::NotImplemented("".to_string())));

        let st: StateTypeFunction =
            Arc::new(|_| Err(DataFusionError::NotImplemented("".to_string())));

        let numeric_datatypes = vec![
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
        ];

        match name {
            "every" => {
                let sig = generate_signatures(vec![numeric_datatypes]);
                let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Boolean)));
                return Some(Arc::new(AggregateUDF::new(name, &sig, &rtf, &acc, &st)));
            }
            "bit_and" | "bit_or" => {
                let sig = generate_signatures(vec![numeric_datatypes]);
                let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Int64)));
                return Some(Arc::new(AggregateUDF::new(name, &sig, &rtf, &acc, &st)));
            }
            "single_value" => {
                let sig = generate_signatures(vec![numeric_datatypes]);
                let rtf: ReturnTypeFunction =
                    Arc::new(|input_types| Ok(Arc::new(input_types[0].clone())));
                return Some(Arc::new(AggregateUDF::new(name, &sig, &rtf, &acc, &st)));
            }
            "regr_count" => {
                let sig = generate_signatures(vec![numeric_datatypes.clone(), numeric_datatypes]);
                let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Int64)));
                return Some(Arc::new(AggregateUDF::new(name, &sig, &rtf, &acc, &st)));
            }
            "regr_syy" | "regr_sxx" => {
                let sig = generate_signatures(vec![numeric_datatypes.clone(), numeric_datatypes]);
                let rtf: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Float64)));
                return Some(Arc::new(AggregateUDF::new(name, &sig, &rtf, &acc, &st)));
            }
            _ => (),
        }

        // Loop through all of the user defined functions
        for schema in self.schemas.values() {
            for (fun_name, func_mutex) in &schema.functions {
                if fun_name.eq(name) {
                    let function = func_mutex.lock().unwrap();
                    if function.aggregation.eq(&false) {
                        return None;
                    }
                    let sig = {
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
                        match function.return_types.get(&input_types.to_vec()) {
                            Some(return_type) => Ok(Arc::new(return_type.clone())),
                            None => Err(DataFusionError::Plan(format!(
                                "UDAF signature not found for input types {input_types:?}"
                            ))),
                        }
                    });
                    return Some(Arc::new(AggregateUDF::new(fun_name, &sig, &rtf, &acc, &st)));
                }
            }
        }

        None
    }

    fn get_variable_type(&self, _: &[String]) -> Option<DataType> {
        unimplemented!("RUST: get_variable_type is not yet implemented for DaskSQLContext")
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }
}

#[pymethods]
impl DaskSQLContext {
    #[new]
    pub fn new(default_catalog_name: &str, default_schema_name: &str) -> Self {
        Self {
            current_catalog: default_catalog_name.to_owned(),
            current_schema: default_schema_name.to_owned(),
            schemas: HashMap::new(),
            options: ConfigOptions::new(),
        }
    }

    /// Change the current schema
    pub fn use_schema(&mut self, schema_name: &str) -> PyResult<()> {
        if self.schemas.contains_key(schema_name) {
            self.current_schema = schema_name.to_owned();
            Ok(())
        } else {
            Err(py_runtime_err(format!(
                "Schema: {schema_name} not found in DaskSQLContext"
            )))
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
                "Schema: {schema_name} not found in DaskSQLContext"
            ))),
        }
    }

    /// Parses a SQL string into an AST presented as a Vec of Statements
    pub fn parse_sql(&self, sql: &str) -> PyResult<Vec<statement::PyStatement>> {
        debug!("parse_sql - '{}'", sql);
        let dd: DaskDialect = DaskDialect {};
        match DaskParser::parse_sql_with_dialect(sql, &dd) {
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
                        .optimize(existing_plan.original_plan)
                        .map(|k| PyLogicalPlan {
                            original_plan: k,
                            current_node: None,
                        })
                        .map_err(py_optimization_exp)
                } else {
                    // This LogicalPlan does not support Optimization. Return original
                    warn!("This LogicalPlan does not support Optimization. Returning original");
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
                    schema_name: create_model.schema_name,
                    model_name: create_model.model_name,
                    input: self._logical_relational_algebra(create_model.select)?,
                    if_not_exists: create_model.if_not_exists,
                    or_replace: create_model.or_replace,
                    with_options: create_model.with_options,
                }),
            })),
            DaskStatement::CreateExperiment(create_experiment) => {
                Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(CreateExperimentPlanNode {
                        schema_name: create_experiment.schema_name,
                        experiment_name: create_experiment.experiment_name,
                        input: self._logical_relational_algebra(create_experiment.select)?,
                        if_not_exists: create_experiment.if_not_exists,
                        or_replace: create_experiment.or_replace,
                        with_options: create_experiment.with_options,
                    }),
                }))
            }
            DaskStatement::PredictModel(predict_model) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(PredictModelPlanNode {
                    schema_name: predict_model.schema_name,
                    model_name: predict_model.model_name,
                    input: self._logical_relational_algebra(predict_model.select)?,
                }),
            })),
            DaskStatement::DescribeModel(describe_model) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(DescribeModelPlanNode {
                    schema: Arc::new(DFSchema::empty()),
                    schema_name: describe_model.schema_name,
                    model_name: describe_model.model_name,
                }),
            })),
            DaskStatement::CreateCatalogSchema(create_schema) => {
                Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(CreateCatalogSchemaPlanNode {
                        schema: Arc::new(DFSchema::empty()),
                        schema_name: create_schema.schema_name,
                        if_not_exists: create_schema.if_not_exists,
                        or_replace: create_schema.or_replace,
                    }),
                }))
            }
            DaskStatement::CreateTable(create_table) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CreateTablePlanNode {
                    schema: Arc::new(DFSchema::empty()),
                    schema_name: create_table.schema_name,
                    table_name: create_table.table_name,
                    if_not_exists: create_table.if_not_exists,
                    or_replace: create_table.or_replace,
                    with_options: create_table.with_options,
                }),
            })),
            DaskStatement::ExportModel(export_model) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(ExportModelPlanNode {
                    schema: Arc::new(DFSchema::empty()),
                    schema_name: export_model.schema_name,
                    model_name: export_model.model_name,
                    with_options: export_model.with_options,
                }),
            })),
            DaskStatement::DropModel(drop_model) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(DropModelPlanNode {
                    schema_name: drop_model.schema_name,
                    model_name: drop_model.model_name,
                    if_exists: drop_model.if_exists,
                    schema: Arc::new(DFSchema::empty()),
                }),
            })),
            DaskStatement::ShowSchemas(show_schemas) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(ShowSchemasPlanNode {
                    schema: Arc::new(DFSchema::empty()),
                    catalog_name: show_schemas.catalog_name,
                    like: show_schemas.like,
                }),
            })),
            DaskStatement::ShowTables(show_tables) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(ShowTablesPlanNode {
                    schema: Arc::new(DFSchema::empty()),
                    catalog_name: show_tables.catalog_name,
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
            DaskStatement::ShowModels(show_models) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(ShowModelsPlanNode {
                    schema: Arc::new(DFSchema::empty()),
                    schema_name: show_models.schema_name,
                }),
            })),
            DaskStatement::DropSchema(drop_schema) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(DropSchemaPlanNode {
                    schema: Arc::new(DFSchema::empty()),
                    schema_name: drop_schema.schema_name,
                    if_exists: drop_schema.if_exists,
                }),
            })),
            DaskStatement::UseSchema(use_schema) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(UseSchemaPlanNode {
                    schema: Arc::new(DFSchema::empty()),
                    schema_name: use_schema.schema_name,
                }),
            })),
            DaskStatement::AnalyzeTable(analyze_table) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(AnalyzeTablePlanNode {
                    schema: Arc::new(DFSchema::empty()),
                    table_name: analyze_table.table_name,
                    schema_name: analyze_table.schema_name,
                    columns: analyze_table.columns,
                }),
            })),
            DaskStatement::AlterTable(alter_table) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(AlterTablePlanNode {
                    schema: Arc::new(DFSchema::empty()),
                    old_table_name: alter_table.old_table_name,
                    new_table_name: alter_table.new_table_name,
                    schema_name: alter_table.schema_name,
                    if_exists: alter_table.if_exists,
                }),
            })),
            DaskStatement::AlterSchema(alter_schema) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(AlterSchemaPlanNode {
                    schema: Arc::new(DFSchema::empty()),
                    old_schema_name: alter_schema.old_schema_name,
                    new_schema_name: alter_schema.new_schema_name,
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

fn generate_signatures(cartesian_setup: Vec<Vec<DataType>>) -> Signature {
    let mut exact_vector = vec![];
    let mut datatypes_iter = cartesian_setup.iter();
    // First pass
    if let Some(first_iter) = datatypes_iter.next() {
        for datatype in first_iter {
            exact_vector.push(vec![datatype.clone()]);
        }
    }
    // Generate the Cartesian product
    for iter in datatypes_iter {
        let mut outer_temp = vec![];
        for outer_datatype in exact_vector {
            for inner_datatype in iter {
                let mut inner_temp = outer_datatype.clone();
                inner_temp.push(inner_datatype.clone());
                outer_temp.push(inner_temp);
            }
        }
        exact_vector = outer_temp;
    }

    // Create vector of TypeSignatures
    let mut one_of_vector = vec![];
    for vector in exact_vector.iter() {
        one_of_vector.push(TypeSignature::Exact(vector.clone()));
    }

    Signature::one_of(one_of_vector.clone(), Volatility::Immutable)
}

#[cfg(test)]
mod test {
    use datafusion::arrow::datatypes::DataType;
    use datafusion_expr::{Signature, TypeSignature, Volatility};

    use crate::sql::generate_signatures;

    #[test]
    fn test_generate_signatures() {
        let sig = generate_signatures(vec![
            vec![DataType::Int64, DataType::Float64],
            vec![DataType::Utf8, DataType::Int64],
        ]);
        let expected = Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Int64, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Float64, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Float64, DataType::Int64]),
            ],
            Volatility::Immutable,
        );
        assert_eq!(sig, expected);
    }
}
