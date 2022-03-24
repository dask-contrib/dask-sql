
use std::collections::HashMap;

use pyo3::prelude::*;

use datafusion::sql::parser::{DFParser, Statement};
use sqlparser::ast::{Query, Select};

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use datafusion::catalog::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::sql::planner::{SqlToRel};
use datafusion::logical_plan::plan::{
    LogicalPlan,
    Projection,
    Filter,
    Window,
    Aggregate,
    Sort,
    Join,
    CrossJoin,
    Repartition,
    Union,
    TableScan,
    EmptyRelation,
    Limit,
    CreateExternalTable,
    CreateMemoryTable,
    DropTable,
    Values,
    Explain,
    Analyze,
    Extension
};

use std::sync::Arc;

use crate::expression::PyExpr;


#[pyclass(name = "LogicalPlanGenerator", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct LogicalPlanGenerator {
    // Holds the ordered plan steps
    #[pyo3(get)]
    pub plan_steps: Vec<String>,
    pub projection: Option<Projection>,
    pub table_scan: Option<TableScan>,
}

impl Default for LogicalPlanGenerator {
    fn default() -> LogicalPlanGenerator {
        LogicalPlanGenerator {
            plan_steps: Vec::new(),
            projection: None,
            table_scan: None,
        }
    }
}

#[pymethods]
impl LogicalPlanGenerator {

    pub fn get_named_projects(&self) -> Vec<PyExpr> {
        match &self.projection {
            Some(proj) => {
                let mut exprs:Vec<PyExpr> = Vec::new();
                for expr in &proj.expr {
                    exprs.push(expr.clone().into());
                }
                exprs
            },
            None => panic!("There is no Projection node present in the Logical Plan!")
        }
    }
}

impl datafusion::logical_plan::plan::PlanVisitor for LogicalPlanGenerator {
    type Error = String;

    fn pre_visit(
        &mut self,
        _plan: &LogicalPlan,
    ) -> std::result::Result<bool, Self::Error> {
        Ok(true)
    }

    /// By inserting in `post_visit` we effectively create a depth first traversal of the SQL parsed tree
    fn post_visit(
        &mut self,
        plan: &LogicalPlan,
    ) -> std::result::Result<bool, Self::Error> {
        let s = match plan {
            LogicalPlan::Projection(projection) => { self.projection = Some(projection.clone()); "Projection" },
            LogicalPlan::Filter { .. } => "Filter",
            LogicalPlan::Window { .. } => "Window",
            LogicalPlan::Aggregate { .. } => "Aggregate",
            LogicalPlan::Sort { .. } => "Sort",
            LogicalPlan::Join { .. } => "Join",
            LogicalPlan::CrossJoin { .. } => "CrossJoin",
            LogicalPlan::Repartition { .. } => "Repartition",
            LogicalPlan::Union { .. } => "Union",
            LogicalPlan::TableScan(table_scan) => { self.table_scan = Some(table_scan.clone()); "TableScan" },
            LogicalPlan::EmptyRelation { .. } => "EmptyRelation",
            LogicalPlan::Limit { .. } => "Limit",
            LogicalPlan::CreateExternalTable { .. } => "CreateExternalTable",
            LogicalPlan::CreateMemoryTable { .. } => "CreateMemoryTable",
            LogicalPlan::DropTable { .. } => "DropTable",
            LogicalPlan::Values { .. } => "Values",
            LogicalPlan::Explain { .. } => "Explain",
            LogicalPlan::Analyze { .. } => "Analyze",
            LogicalPlan::Extension { .. } => "Extension",
        };

        self.plan_steps.push(s.into());
        Ok(true)
    }
}



/// DaskSQLContext is main interface used for interacting with Datafusion to
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
#[derive(Clone)]
pub struct DaskSQLContext {
    default_schema_name: String,
    pub schemas: HashMap<String, DaskSchema>,
}

impl datafusion::sql::planner::ContextProvider for DaskSQLContext {
    fn get_table_provider(
        &self,
        name: TableReference,
    ) -> Option<Arc<dyn TableProvider>> {
        match self.schemas.get(&String::from(&self.default_schema_name)) {
            Some(schema) => {
                let mut resp = None;
                for (_table_name, table) in &schema.tables {
                    if table.name.eq(&name.table()) {
                        // Build the Schema here
                        let mut fields: Vec<Field> = Vec::new();

                        // Iterate through the DaskTable instance and create a Schema instance
                        for (column_name, column_type) in &table.columns {
                            fields.push(Field::new(column_name, column_type.sqlType.clone(), false));
                        }

                        resp = Some(Schema::new(fields));
                    }
                }
                Some(Arc::new(datafusion::datasource::empty::EmptyTable::new(
                    Arc::new(
                        resp.unwrap()
                    )
                )))
            },
            None => panic!("Schema with name {} not found", "table_name"),
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<datafusion::physical_plan::udf::ScalarUDF>> {
        println!("RUST: get_function_meta");
        let _f: datafusion::physical_plan::functions::ScalarFunctionImplementation =
            Arc::new(|_| Err(datafusion::error::DataFusionError::NotImplemented("".to_string())));
        match name {
            _ => None,
        }
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<datafusion::physical_plan::udaf::AggregateUDF>> {
        println!("RUST: get_aggregate_meta");
        unimplemented!()
    }
}


#[pymethods]
impl DaskSQLContext {
    #[new]
    pub fn new(default_schema_name: String) -> Self {
        Self {
            default_schema_name: default_schema_name,
            schemas: HashMap::new(),
        }
    }

    pub fn register_schema(&mut self, schema_name:String, schema: DaskSchema) {
        self.schemas.insert(schema_name, schema);
    }

    pub fn register_table(&mut self, schema_name:String, table: DaskTable) {
        match self.schemas.get_mut(&schema_name) {
            Some(schema) => schema.add_table(table),
            None => println!("Schema: {} not found in DaskSQLContext", schema_name),
        }
    }

    /// Parses a SQL string into an AST presented as a Vec of Statements
    pub fn parse_sql(&self, sql: &str) -> Vec<PyStatement> {
        match DFParser::parse_sql(sql) {
            Ok(k) => {
                let mut statements = Vec::new();
                for statement in k {
                    statements.push(statement.into());
                }
                statements
            },
            Err(e) => panic!("{}", e.to_string()),
        }
    }

    /// Creates a non-optimized Relational Algebra LogicalPlan from an AST Statement
    pub fn logical_relational_algebra(&self, statement: PyStatement) -> PyLogicalPlan {
        let planner = SqlToRel::new(self);

        match planner.statement_to_plan(&statement.statement) {
            Ok(k) => {
                println!("Full Logical Plan: {:?}", k);
                PyLogicalPlan {
                    original_plan: k,
                    current_node: None,
                }
            },
            Err(e) => panic!("{}", e.to_string()),
        }
    }
}


#[pyclass(name = "LogicalPlan", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct PyLogicalPlan {
    /// The orginal LogicalPlan that was parsed by DataFusion from the input SQL
    original_plan: LogicalPlan,
    /// The original_plan is traversed. current_node stores the current node of this traversal
    current_node: Option<LogicalPlan>,
}

impl From<PyLogicalPlan> for LogicalPlan {
    fn from(logical_plan: PyLogicalPlan) -> LogicalPlan  {
        logical_plan.original_plan
    }
}

impl From<LogicalPlan> for PyLogicalPlan {
    fn from(logical_plan: LogicalPlan) -> PyLogicalPlan {
        PyLogicalPlan {
            original_plan: logical_plan,
            current_node: None,
        }
    }
}


/// Traverses the logical plan to locate the Table associated with the query
fn table_from_logical_plan(plan: &LogicalPlan) -> Option<DaskTable> {
    match plan {
        datafusion::logical_plan::plan::LogicalPlan::Projection(projection) => {
            println!("Projection Input: {:?}", &projection.input);
            table_from_logical_plan(&projection.input)
        },
        datafusion::logical_plan::plan::LogicalPlan::Filter(filter) => {
            println!("Filter Input: {:?}", &filter.input);
            table_from_logical_plan(&filter.input)
        },
        datafusion::logical_plan::plan::LogicalPlan::Window(_window) => {
            println!("Window");
            None
        },
        datafusion::logical_plan::plan::LogicalPlan::Aggregate(_aggregate) => {
            println!("Aggregate");
            None
        },
        datafusion::logical_plan::plan::LogicalPlan::Sort(_sort) => {
            println!("Sort");
            None
        },
        datafusion::logical_plan::plan::LogicalPlan::Join(_join) => {
            println!("Join");
            None
        },
        datafusion::logical_plan::plan::LogicalPlan::CrossJoin(_crossJoin) => {
            println!("CrossJoin");
            None
        },
        datafusion::logical_plan::plan::LogicalPlan::TableScan(tableScan) => {

            // Get the TableProvider for this Table instance
            let tbl_provider: Arc<dyn TableProvider> = tableScan.source.clone();
            let tbl_schema: SchemaRef = tbl_provider.schema();
            let fields = tbl_schema.fields();

            let mut cols: Vec<(String, DaskRelDataType)> = Vec::new();
            for field in fields {
                cols.push(
                    (
                        String::from(field.name()),
                        DaskRelDataType {
                            name: String::from(field.name()),
                            sqlType: field.data_type().clone(),
                        }
                    )
                );
            }

            Some(DaskTable {
                name: String::from(&tableScan.table_name),
                statistics: DaskStatistics { row_count: 0.0 },
                columns: cols,
            })
        },
        _ => {
            panic!("Ok something went wrong here!!!")
        }
    }
}

/// Unfortunately PyO3 forces us to do this as placing these methods in the #[pymethods] version
/// of `impl PyLogicalPlan` causes issues with types not properly being mapped to Python from Rust
impl PyLogicalPlan {
    /// Getter method for the LogicalPlan, if current_node is None return original_plan.
    fn current_node(&mut self) -> LogicalPlan {
        match &self.current_node {
            Some(current) => current.clone(),
            None => {
                self.current_node = Some(self.original_plan.clone());
                self.current_node.clone().unwrap()
            },
        }
    }
}


#[pymethods]
impl PyLogicalPlan {

    /// Projection: Gets the names of the fields that should be projected
    fn get_named_projects(&mut self) -> PyResult<Vec<PyExpr>> {
        match self.current_node() {
            LogicalPlan::Projection(projection) => {
                let mut projs: Vec<PyExpr> = Vec::new();
                for expr in projection.expr {
                    projs.push(expr.clone().into());
                }
                Ok(projs)
            },
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>("current_node is not of type Projection")),
        }
    }

    /// Gets the "input" for the current LogicalPlan
    pub fn get_inputs(&mut self) -> PyResult<Vec<PyLogicalPlan>> {
        let mut py_inputs: Vec<PyLogicalPlan> = Vec::new();
        for input in self.current_node().inputs() {
            py_inputs.push(input.clone().into());
        }
        Ok(py_inputs)
    }

    /// Examines the current_node and get the fields associated with it
    pub fn get_field_names(&mut self) -> PyResult<Vec<String>> {
        let mut field_names: Vec<String> = Vec::new();
        for field in self.current_node().schema().fields() {
            field_names.push(String::from(field.name()));
        }
        Ok(field_names)
    }


    /// If the LogicalPlan represents access to a Table that instance is returned
    /// otherwise None is returned
    pub fn table(&mut self) -> PyResult<DaskTable> {
        match table_from_logical_plan(&self.current_node()) {
            Some(table) => Ok(table),
            None => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>("Unable to compute DaskTable from Datafusion LogicalPlan")),
        }
    }


    /// Gets the Relation "type" of the current node. Ex: Projection, TableScan, etc
    pub fn get_current_node_type(&mut self) -> PyResult<String> {
        match self.current_node() {
            LogicalPlan::Projection(_projection) => Ok(String::from("Projection")),
            LogicalPlan::Filter(_filter) => Ok(String::from("Filter")),
            LogicalPlan::Window(_window) => Ok(String::from("Window")),
            LogicalPlan::Aggregate(_aggregate) => Ok(String::from("Aggregate")),
            LogicalPlan::Sort(_sort) => Ok(String::from("Sort")),
            LogicalPlan::Join(_join) => Ok(String::from("Join")),
            LogicalPlan::CrossJoin(_cross_join) => Ok(String::from("CrossJoin")),
            LogicalPlan::Repartition(_repartition) => Ok(String::from("Repartition")),
            LogicalPlan::Union(_union) => Ok(String::from("Union")),
            LogicalPlan::TableScan(_table_scan) => Ok(String::from("TableScan")),
            LogicalPlan::EmptyRelation(_empty_relation) => Ok(String::from("EmptyRelation")),
            LogicalPlan::Limit(_limit) => Ok(String::from("Limit")),
            LogicalPlan::CreateExternalTable(_create_external_table) => Ok(String::from("CreateExternalTable")),
            LogicalPlan::CreateMemoryTable(_create_memory_table) => Ok(String::from("CreateMemoryTable")),
            LogicalPlan::DropTable(_drop_table) => Ok(String::from("DropTable")),
            LogicalPlan::Values(_values) => Ok(String::from("Values")),
            LogicalPlan::Explain(_explain) => Ok(String::from("Explain")),
            LogicalPlan::Analyze(_analyze) => Ok(String::from("Analyze")),
            LogicalPlan::Extension(_extension) => Ok(String::from("Extension")),
        }
    }


    /// Explain plan for the full and original LogicalPlan
    pub fn explain_original(&self) -> PyResult<String> {
        Ok(format!("{}", self.original_plan.display_indent()))
    }


    /// Explain plan from the current node onward
    pub fn explain_current(&mut self) -> PyResult<String> {
        Ok(format!("{}", self.current_node().display_indent()))
    }


    // pub fn plan_generator(&self) -> LogicalPlanGenerator {
    //     // Actually gonna test out walking the plan here ....
    //     let mut visitor = LogicalPlanGenerator::default();
    //     self.logical_plan.accept(&mut visitor);
    //     // visitor.plan_steps.clone()
    //     visitor
    // }

    // pub fn getCondition(&self) -> Filter {
    //     match &self.logical_plan {
    //         Filter(filter) => {
    //             filter
    //         },
    //         _ => panic!("Something wrong here")
    //     }
    // }
}


#[pyclass(name = "Statement", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct PyStatement {
    pub statement: Statement,
}

impl From<PyStatement> for Statement {
    fn from(statement: PyStatement) -> Statement  {
        statement.statement
    }
}

impl From<Statement> for PyStatement {
    fn from(statement: Statement) -> PyStatement {
        PyStatement { statement }
    }
}


impl PyStatement {
    pub fn new(statement: Statement) -> Self {
        Self { statement }
    }
}


#[pymethods]
impl PyStatement {

    #[staticmethod]
    pub fn table_name() -> String {
        String::from("Got here!!!")
    }
}

#[pyclass(name = "Query", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct PyQuery {
    pub(crate) query: Query,
}

impl From<PyQuery> for Query {
    fn from(query: PyQuery) -> Query  {
        query.query
    }
}

impl From<Query> for PyQuery {
    fn from(query: Query) -> PyQuery {
        PyQuery { query }
    }
}

#[pyclass(name = "DaskSQLNode", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct DaskSQLNode {
    pub(crate) statements: Vec<Statement>,
}


#[pyclass(name = "Select", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct PySelect {
    pub(crate) select: Select,
}

impl From<PySelect> for Select {
    fn from(select: PySelect) -> Select  {
        select.select
    }
}

impl From<Select> for PySelect {
    fn from(select: Select) -> PySelect {
        PySelect { select }
    }
}


#[pyclass(name = "DaskSchema", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct DaskSchema {
    #[pyo3(get, set)]
    name: String,
    tables: HashMap<String, DaskTable>,
    functions: HashMap<String, DaskFunction>,
}

#[pymethods]
impl DaskSchema {
    #[new]
    pub fn new(schema_name: String) -> Self {
        Self {
            name: schema_name,
            tables: HashMap::new(),
            functions: HashMap::new(),
        }
    }

    pub fn to_string(&self) -> String {
        format!("Schema Name: ({}) - # Tables: ({}) - # Custom Functions: ({})", &self.name, &self.tables.len(), &self.functions.len())
    }

    pub fn add_table(&mut self, table: DaskTable) {
        self.tables.insert(table.name.clone(), table);
    }
}

// #[pyclass(name = "DaskSqlTypeName", module = "dask_planner", subclass)]
// #[derive(Debug, Clone)]
// pub struct DaskSqlTypeName {
//     name: String,
// }

/// Converts a SQL type represented as a String that is received from Python,
/// Sending from Python to Rust as a String is the easiest path to do this.
/// From there it is converted to a Arrow DataType instance.
fn sql_type_str_to_datatype(str_sql_type: String) -> DataType {

    if str_sql_type.starts_with("timestamp") {
        println!("THIS!!! {:?}", str_sql_type);
        DataType::Timestamp(TimeUnit::Millisecond, Some(String::from("America/New_York")))
    } else {
        match &str_sql_type[..] {
            "NULL" => {
                DataType::Null
            },
            "BOOLEAN" => {
                DataType::Boolean
            },
            "TINYINT" => {
                DataType::Int8
            },
            "SMALLINT" => {
                DataType::Int16
            },
            "INTEGER" => {
                DataType::Int32
            },
            "BIGINT" => {
                DataType::Int64
            },
            "FLOAT" => {
                DataType::Float32
            },
            "DOUBLE" => {
                DataType::Float64
            },
            "VARCHAR" => {
                DataType::Utf8
            },
            "TIMESTAMP" => {
                DataType::Timestamp(TimeUnit::Millisecond, Some(String::from("America/New_York")))
            },
        // Date32,
        // Date64,
        // Time32(TimeUnit),
        // Time64(TimeUnit),
        // Duration(TimeUnit),
        // Interval(IntervalUnit),
        // Binary,
        // FixedSizeBinary(i32),
        // LargeBinary,
        // Utf8,
        // LargeUtf8,
        // List(Box<Field>),
        // FixedSizeList(Box<Field>, i32),
        // LargeList(Box<Field>),
        // Struct(Vec<Field>),
        // Union(Vec<Field>, UnionMode),
        // Dictionary(Box<DataType>, Box<DataType>),
        // Decimal(usize, usize),
        // Map(Box<Field>, bool),
            _ => {
                println!("Not yet implemented String value: {:?}", &str_sql_type);
                panic!("This is not yet implemented!!!")
            }
        }
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct DaskRelDataType {
    name: String,
    sqlType: DataType,
}

#[pymethods]
impl DaskRelDataType {

    #[new]
    pub fn new(field_name: String, column_str_sql_type: String) -> Self {
        DaskRelDataType {
            name: field_name,
            sqlType: sql_type_str_to_datatype(column_str_sql_type),
        }
    }

    pub fn get_column_name(&self) -> String {
        String::from(self.name.clone())
    }

    pub fn get_type(&self) -> DataType {
        self.sqlType.clone()
    }

    pub fn get_type_as_str(&self) -> String {
        match self.sqlType {
            DataType::Null => {
                String::from("NULL")
            },
            DataType::Boolean => {
                String::from("BOOLEAN")
            },
            DataType::Int8 => {
                String::from("TINYINT")
            },
            DataType::UInt8 => {
                String::from("TINYINT")
            },
            DataType::Int16 => {
                String::from("SMALLINT")
            },
            DataType::UInt16 => {
                String::from("SMALLINT")
            },
            DataType::Int32 => {
                String::from("INTEGER")
            },
            DataType::UInt32 => {
                String::from("INTEGER")
            },
            DataType::Int64 => {
                String::from("BIGINT")
            },
            DataType::UInt64 => {
                String::from("BIGINT")
            },
            DataType::Float32 => {
                String::from("FLOAT")
            },
            DataType::Float64 => {
                String::from("DOUBLE")
            },
            DataType::Timestamp{..} => {
                String::from("TIMESTAMP")
            },
            DataType::Date32 => {
                String::from("DATE32")
            },
            DataType::Date64 => {
                String::from("DATE64")
            },
            DataType::Time32(..) => {
                String::from("TIME32")
            },
            DataType::Time64(..) => {
                String::from("TIME64")
            },
            DataType::Duration(..) => {
                String::from("DURATION")
            },
            DataType::Interval(..) => {
                String::from("INTERVAL")
            },
            DataType::Binary => {
                String::from("BINARY")
            },
            DataType::FixedSizeBinary(..) => {
                String::from("FIXEDSIZEBINARY")
            },
            DataType::LargeBinary => {
                String::from("LARGEBINARY")
            },
            DataType::Utf8 => {
                String::from("VARCHAR")
            },
            DataType::LargeUtf8 => {
                String::from("BIGVARCHAR")
            },
            DataType::List(..) => {
                String::from("LIST")
            },
            DataType::FixedSizeList(..) => {
                String::from("FIXEDSIZELIST")
            },
            DataType::LargeList(..) => {
                String::from("LARGELIST")
            },
            DataType::Struct(..) => {
                String::from("STRUCT")
            },
            DataType::Union(..) => {
                String::from("UNION")
            },
            DataType::Dictionary(..) => {
                String::from("DICTIONARY")
            },
            DataType::Decimal(..) => {
                String::from("DECIMAL")
            },
            DataType::Map(..) => {
                String::from("MAP")
            },
            _ => {
                panic!("This is not yet implemented!!!")
            }
        }
    }

    // pub fn getFieldList(&self) -> Vec<DaskRelDataTypeField> {
    //     let mut fields = Vec::new();
    //     fields.push(DaskRelDataTypeField {
    //         index: 0,
    //         name: String::from("id"),
    //         // field_type: Option,
    //         dynamic_star: false,
    //     });
    //     fields
    // }
}

// #[pyclass]
// #[derive(Debug, Clone)]
// pub struct DaskRelDataTypeField {
//     index: i16,
//     name: String,
//     // field_type: DaskRelDataType,
//     dynamic_star: bool,
// }

// #[pymethods]
// impl DaskRelDataTypeField {

//     #[new]
//     pub fn new(index: i16, name: String) -> Self {
//         Self {
//             index: index,
//             name: name,
//             dynamic_star: false,
//         }
//     }

//     pub fn get_index(&self) -> i16 {
//         self.index
//     }

//     pub fn get_type(&self) -> DaskRelDataType {
//         DaskRelDataType {
//             field_names: vec![String::from("STRING")]
//         }
//     }
// }

#[pyclass(name = "DaskTable", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct DaskTable {
    name: String,
    statistics: DaskStatistics,
    columns: Vec<(String, DaskRelDataType)>,
}


#[pymethods]
impl DaskTable {

    #[new]
    pub fn new(table_name: String, row_count: f64) -> Self {
        Self {
            name: table_name,
            statistics: DaskStatistics::new(row_count),
            columns: Vec::new(),
        }
    }

    pub fn to_string(&self) -> String {
        format!("Table Name: ({})", &self.name)
    }


    pub fn add_column(&mut self, column_name: String, column_type_str: String) {
        let sqlType: DaskRelDataType = DaskRelDataType {
            name: String::from(&column_name),
            sqlType: sql_type_str_to_datatype(column_type_str),
        };

        self.columns.push((column_name, sqlType));
    }

    pub fn get_qualified_name(&self) -> Vec<String> {
        let mut qualified_name = Vec::new();
        //TODO: What scope should the current schema be pulled from here?? For now temporary hardcoded to default "root"
        qualified_name.push(String::from("root"));
        qualified_name.push(self.name.clone());
        qualified_name
    }

    pub fn column_names(&self) -> Vec<String> {
        let mut cns:Vec<String> = Vec::new();
        for c in &self.columns {
            cns.push(String::from(&c.0));
        }
        cns
    }

    pub fn column_types(&self) -> Vec<DaskRelDataType> {
        let mut col_types: Vec<DaskRelDataType> = Vec::new();
        for col in &self.columns {
            col_types.push(col.1.clone())
        }
        col_types
    }

    pub fn num_columns(&self) {
        println!("There are {} columns in table {}", self.columns.len(), self.name);
    }

    // pub fn get_row_type(&self) -> DaskRelDataType {
    //     let mut field_names: Vec<String> = Vec::with_capacity(self.columns.len());
    //     for field in &self.columns {
    //         field_names.push(String::from(&field.0));
    //     }
    //     DaskRelDataType::new(field_names)
    // }
}

#[pyclass(name = "DaskFunction", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct DaskFunction {
    name: String,
}

#[pyclass(name = "DaskStatistics", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct DaskStatistics {
    row_count: f64,
}

#[pymethods]
impl DaskStatistics {
    #[new]
    pub fn new(row_count: f64) -> Self {
        Self {
            row_count: row_count,
        }
    }
}
