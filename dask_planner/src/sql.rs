
use std::collections::HashMap;

use std::fmt;

use pyo3::prelude::*;

use parking_lot::Mutex;

use datafusion::sql::parser::{DFParser, Statement};
use sqlparser::ast::{Query, Select};

use datafusion::catalog::catalog::{CatalogList};

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use datafusion::catalog::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::{DFSchema, Expr};
use datafusion::logical_plan::plan::LogicalPlan;
use datafusion::sql::planner::{SqlToRel};

use std::sync::Arc;



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
                for (table_name, table) in &schema.tables {
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
                PyLogicalPlan { 
                    logical_plan: k,
                }
            },
            Err(e) => panic!("{}", e.to_string()),
        }
    }
}


#[pyclass(name = "LogicalPlan", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct PyLogicalPlan {
    pub logical_plan: LogicalPlan,
}

impl From<PyLogicalPlan> for LogicalPlan {
    fn from(logical_plan: PyLogicalPlan) -> LogicalPlan  {
        logical_plan.logical_plan
    }
}

impl From<LogicalPlan> for PyLogicalPlan {
    fn from(logical_plan: LogicalPlan) -> PyLogicalPlan {
        PyLogicalPlan { 
            logical_plan: logical_plan,
        }
    }
}


#[pymethods]
impl PyLogicalPlan {
    pub fn get_field_names(&self) -> Vec<String> {
        let mut field_names: Vec<String> = Vec::new();

        for field in self.logical_plan.schema().fields() {
            field_names.push(String::from(field.name()));
        }

        // // Get all of the Expressions from the parsed logical plan
        // let exprs = self.logical_plan.expressions();
        // println!("EXPRESSIONS: {:?}", exprs);

        // for expr in exprs {
        //     println!("EXPR: {:?}", expr);
        //     match expr {
        //         Expr::Alias( .. ) => println!("Alias was encountered!"),
        //         Expr::Column(column) => {
        //             println!("Column Name: {:?}", column.name);
        //             if let Some(relation) = column.relation {
        //                 println!("Relation: {:?}", relation);
        //             } else {
        //                 println!("Column has no relation present. AKA None(E)");
        //             }
        //         },
        //         Expr::ScalarVariable( .. ) => println!("ScalarVariable was encountered!"),
        //         Expr::Literal( .. ) => println!("Literal was encountered!"),
        //         Expr::BinaryExpr{ .. } => println!("BinaryExpr was encountered!"),
        //         Expr::Not( .. ) => println!("Not was encountered!"),
        //         Expr::IsNotNull( .. ) => println!("IsNotNull was encountered!"),
        //         Expr::IsNull( .. ) => println!("IsNull was encountered!"),
        //         Expr::Negative( .. ) => println!("Negative was encountered!"),
        //         Expr::AggregateFunction{ .. } => {
        //             println!("Aggregation function!!!!!");
        //         },
        //         Expr::Wildcard => println!("Wildcard was encountered!"),
        //         _ => println!("Nothing matched ...."),
        //     }
        // }

        field_names
    }

    /// If the LogicalPlan represents access to a Table that instance is returned
    /// otherwise None is returned
    pub fn table(&self) -> PyResult<DaskTable> {
        match &self.logical_plan {
            datafusion::logical_plan::plan::LogicalPlan::Projection(projection) => {
                match &*projection.input {
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

                        Ok(DaskTable {
                            name: String::from(&tableScan.table_name),
                            statistics: DaskStatistics { row_count: 0.0 },
                            columns: cols,
                        })
                    },
                    _ => panic!("Was something not in the list!")
                }
            },
            datafusion::logical_plan::plan::LogicalPlan::TableScan(tableScan) => {
                Ok(DaskTable {
                    name: String::from(&tableScan.table_name),
                    statistics: DaskStatistics { row_count: 0.0 },
                    columns: Vec::new(),
                })
            },
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>("Not yet implemented"))
        }
    }

    pub fn explain(&self) -> String {
        format!("{}", self.logical_plan.display_indent())
    }
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

#[pyclass]
#[derive(Debug, Clone)]
pub struct DaskRelDataType {
    name: String,
    sqlType: DataType,
}

#[pymethods]
impl DaskRelDataType {

    #[new]
    pub fn new(field_name: String) -> Self {
        DaskRelDataType {
            name: field_name,
            sqlType: DataType::Int64,
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
            _ => {
                panic!("This is not yet implemented!!!")
            }


    // Float16,
    // Float32,
    // Float64,
    // Timestamp(TimeUnit, Option<String>),
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

    //TODO: Need to include the SqlTypeName later, for now in a hurry to get POC done
    //pub fn addColumn(&mut self, column_name: String, column_type: DaskRelDataType) {
    pub fn add_column(&mut self, column_name: String) {

        let sqlType: DaskRelDataType = DaskRelDataType {
            name: String::from(&column_name),
            sqlType: DataType::Int64,
        };

        self.columns.push((column_name, sqlType));
    }

    pub fn get_qualified_name(&self) -> Vec<String> {
        let mut qualified_name = Vec::new();
        //TODO: Don't hardcode this. Need to figure out what scope this value is pulled from however???
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
