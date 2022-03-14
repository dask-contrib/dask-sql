
use std::collections::HashMap;

use pyo3::prelude::*;

use parking_lot::Mutex;

use datafusion::sql::parser::{DFParser, Statement};
use sqlparser::ast::{Query, Select};
use datafusion::logical_plan::Expr;

use datafusion::catalog::catalog::{CatalogList};

use datafusion::arrow::datatypes::{DataType, Field, Schema};

use datafusion::catalog::TableReference;
use datafusion::datasource::TableProvider;
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
    pub state: Arc<Mutex<DaskSQLContextState>>,
}

#[pymethods]
impl DaskSQLContext {
    #[new]
    pub fn new() -> Self {
        Self::new()
    }

    /// Parses a SQL string into an AST presented as a Vec of Statements
    pub fn parse_sql(&self, sql: &str) -> Vec<PyStatement> {
        let resp = DFParser::parse_sql(sql).unwrap().clone();
        let mut statements = Vec::new();
        for statement in resp {
            statements.push(statement.into());
        }
        statements
    }

    // /// Creates a non-optimized Relational Algebra LogicalPlan from an AST Statement
    // pub fn logical_relational_algebra(&self, statement: PyStatement) -> PyLogicalPlan {
    //     let context_provider = &DaskSQLContext {};
    //     let planner = SqlToRel::new(context_provider);
    
    //     match planner.statement_to_plan(&statement.statement) {
    //         Ok(k) => {
    //             PyLogicalPlan { 
    //                 logical_plan: k ,
    //                 table: DaskTable{
    //                     name: String::from("test"),
    //                     statistics: DaskStatistics::new(0.0),
    //                     tableColumns: Vec::new(),
    //                     row_type: DaskRelDataType {
    //                         field_names: vec![String::from("id")]
    //                     },
    //                 },
    //             }
    //         },
    //         Err(e) => panic!("{}", e.to_string()),
    //     }
    // }
}


/// Dask SQL context for registering data sources and executing queries
#[derive(Clone)]
pub struct DaskSQLContextState {
    /// Collection of catalogs containing schemas and ultimately TableProviders
    pub catalog_list: Arc<dyn CatalogList>,
    // /// Scalar functions that are registered with the context
    // pub scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    // /// Aggregate functions registered in the context
    // pub aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    // /// Context configuration
    // pub config: ExecutionConfig,
    // /// Execution properties
    // pub execution_props: ExecutionProps,
    // /// Object Store that are registered with the context
    // pub object_store_registry: Arc<ObjectStoreRegistry>,
    // /// Runtime environment
    // pub runtime_env: Arc<RuntimeEnv>,
}


impl datafusion::sql::planner::ContextProvider for DaskSQLContextState {
    fn get_table_provider(
        &self,
        name: TableReference,
    ) -> Option<Arc<dyn TableProvider>> {
        let schema = match name.table() {
            "test" => Some(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
            ])),
            _ => None,
        };
        schema.map(|s| -> Arc<dyn TableProvider> {
            Arc::new(datafusion::datasource::empty::EmptyTable::new(Arc::new(s)))
        })
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<datafusion::physical_plan::udf::ScalarUDF>> {
        let _f: datafusion::physical_plan::functions::ScalarFunctionImplementation =
            Arc::new(|_| Err(datafusion::error::DataFusionError::NotImplemented("".to_string())));
        match name {
            _ => None,
        }
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<datafusion::physical_plan::udaf::AggregateUDF>> {
        unimplemented!()
    }
}


#[pyclass(name = "LogicalPlan", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct PyLogicalPlan {
    pub logical_plan: LogicalPlan,
    pub table: DaskTable,
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
            table: DaskTable{
                name: String::from("test"),
                statistics: DaskStatistics::new(0.0),
                tableColumns: Vec::new(),
                row_type: DaskRelDataType {
                    field_names: vec![String::from("id")]
                },
            },
        }
    }
}


#[pymethods]
impl PyLogicalPlan {
    pub fn getFieldNames(&self) -> Vec<String> {
        let schema = self.logical_plan.schema();
        println!("Schema: {:?}", schema);

        // Get all of the Expressions from the parsed logical plan
        let exprs = self.logical_plan.expressions();
        println!("EXPRESSIONS: {:?}", exprs);

        for expr in exprs {
            println!("EXPR: {:?}", expr);
            match expr {
                Expr::Alias( .. ) => println!("Alias was encountered!"),
                Expr::Column(column) => {
                    println!("Column Name: {:?}", column.name);
                    if let Some(relation) = column.relation {
                        println!("Relation: {:?}", relation);
                    } else {
                        println!("Column has no relation present. AKA None(E)");
                    }
                },
                Expr::ScalarVariable( .. ) => println!("ScalarVariable was encountered!"),
                Expr::Literal( .. ) => println!("Literal was encountered!"),
                Expr::BinaryExpr{ .. } => println!("BinaryExpr was encountered!"),
                Expr::Not( .. ) => println!("Not was encountered!"),
                Expr::IsNotNull( .. ) => println!("IsNotNull was encountered!"),
                Expr::IsNull( .. ) => println!("IsNull was encountered!"),
                Expr::Negative( .. ) => println!("Negative was encountered!"),
                Expr::AggregateFunction{ .. } => {
                    println!("Aggregation function!!!!!");
                },
                Expr::Wildcard => println!("Wildcard was encountered!"),
                _ => println!("Nothing matched ...."),
            }
        }

        let field_names = Vec::new();
        field_names
    }

    pub fn getTable(&self) -> DaskTable {
        self.table.clone()
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

#[pyfunction]
fn query(statement: PyStatement) -> PyResult<PyQuery> {
    Ok(PyQuery {
        query: match statement.statement {
            Statement::Statement(sql_statement) => {
                match *sql_statement {
                   sqlparser::ast::Statement::Query(query) => {
                    //    println!("Query: {:?}", *query);
                       *query
                    },
                    _ => panic!("something didn't go correct here")
                }
            },
            _ => panic!("CreateTableStatement received but it was not expected")
        },
    })
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

#[pyfunction]
fn select(query: PyQuery) -> PyResult<PySelect> {
    println!("Query in select: {:?}", query.query);
    Ok(PySelect {
        select: match query.query.body {
            sqlparser::ast::SetExpr::Select(select) => {
                println!("Select: {:?}", *select);
                *select
                // for si in &select.projection {
                //     match si {
                //         sqlparser::ast::SelectItem::UnnamedExpr(expr) =>  {
                //             match expr {
                //                 sqlparser::ast::Expr::Identifier(ident) => {
                //                     projected_cols.push(String::from(&ident.value))
                //                 },
                //                 _ => println!("Doesn't matter"),
                //             }
                //         },
                //         _ => println!("Doesn't matter"),
                //     }
                // }
            },
            _ => panic!("nothing else matters"),
        },
    })
}


#[pyclass(name = "DaskSchema", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct DaskSchema {
    #[pyo3(get, set)]
    name: String,
    databaseTables: HashMap<String, DaskTable>,
    functions: HashMap<String, DaskFunction>,
}

#[pymethods]
impl DaskSchema {
    #[new]
    pub fn new(schema_name: String) -> Self {
        Self {
            name: schema_name,
            databaseTables: HashMap::new(),
            functions: HashMap::new(),
        }
    }

    pub fn to_string(&self) -> String {
        format!("Schema Name: ({}) - # Tables: ({}) - # Custom Functions: ({})", &self.name, &self.databaseTables.len(), &self.functions.len())
    }

    pub fn addTable(&mut self, table: DaskTable) {
        self.databaseTables.insert(table.name.clone(), table);
    }
}

#[pyclass(name = "DaskSqlTypeName", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct DaskSqlTypeName {
    name: String,
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct DaskRelDataType {
    #[pyo3(get, set)]
    field_names: Vec<String>,
}

#[pymethods]
impl DaskRelDataType {
    pub fn getFieldNames(&self) -> Vec<String> {
        self.field_names.clone()
    }

    pub fn getFieldList(&self) -> Vec<DaskRelDataTypeField> {
        let mut fields = Vec::new();
        fields.push(DaskRelDataTypeField {
            index: 0,
            name: String::from("id"),
            // field_type: Option,
            dynamic_star: false,
        });
        fields
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct DaskRelDataTypeField {
    index: i16,
    name: String,
    // field_type: DaskRelDataType,
    dynamic_star: bool,
}

#[pymethods]
impl DaskRelDataTypeField {
    pub fn getIndex(&self) -> i16 {
        self.index
    }

    pub fn getType(&self) -> DaskRelDataType {
        DaskRelDataType {
            field_names: vec![String::from("STRING")]
        }
    }
}

#[pyclass(name = "DaskTable", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct DaskTable {
    name: String,
    statistics: DaskStatistics,
    tableColumns: Vec<(String, DaskSqlTypeName)>,
    row_type: DaskRelDataType,
}


#[pymethods]
impl DaskTable {
    #[new]
    pub fn new(table_name: String, row_count: f64) -> Self {
        Self {
            name: table_name,
            statistics: DaskStatistics::new(row_count),
            tableColumns: Vec::new(),
            row_type: DaskRelDataType {
                field_names: vec![String::from("id")]
            },
        }
    }

    pub fn to_string(&self) -> String {
        format!("Table Name: ({})", &self.name)
    }

    //TODO: Need to include the SqlTypeName later, for now in a hurry to get POC done
    // pub fn addColumn(&self, column_name: String, column_type: DaskSqlTypeName) {
    pub fn addColumn(&mut self, column_name: String) {
        self.tableColumns.push((column_name, DaskSqlTypeName {name: String::from("string")}));
    }

    pub fn getQualifiedName(&self) -> Vec<String> {
        let mut qualified_name = Vec::new();
        qualified_name.push(String::from("root"));
        qualified_name.push(self.name.clone());
        qualified_name
    }

    pub fn getRowType(&self) -> DaskRelDataType {
        self.row_type.clone()
    }
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

pub fn init_module(m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(query))?;
    m.add_wrapped(wrap_pyfunction!(select))?;
    Ok(())
}
