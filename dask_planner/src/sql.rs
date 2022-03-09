
use std::collections::HashMap;

use pyo3::prelude::*;

use datafusion::sql::parser::{DFParser, Statement};
use sqlparser::ast::{Query, Select};

use datafusion::arrow::datatypes::{DataType, Field, Schema};

use datafusion::catalog::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::plan::LogicalPlan;
use datafusion::sql::planner::{SqlToRel};

use std::sync::Arc;

struct DaskSQLContextProvider {}

impl datafusion::sql::planner::ContextProvider for DaskSQLContextProvider {
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

#[pyfunction]
pub fn getSqlNode(sql: &str) -> Vec<PyStatement> {
    let resp = DFParser::parse_sql(sql).unwrap().clone();
    let mut statements = Vec::new();
    for statement in resp {
        statements.push(statement.into());
    }
    statements
}

#[pyfunction]
pub fn getRelationalAlgebra(statement: PyStatement) -> PyLogicalPlan {
    let context_provider = &DaskSQLContextProvider {};
    let planner = SqlToRel::new(context_provider);

    match planner.statement_to_plan(&statement.statement) {
        Ok(k) => PyLogicalPlan { logical_plan: k },
        Err(e) => panic!("{}", e.to_string()),
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
        PyLogicalPlan { logical_plan }
    }
}


// #[pymethods]
// impl PyLogicalPlan {
//     pub fn getFieldNames() -> Vec<String> {

//     }
// }


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
                       println!("Query: {:?}", *query);
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



// #[pyproto]
// impl PyMappingProtocol for PyStatement {
//     fn __getitem__(&self, key: &str) -> PyResult<PyStatement> {
//         Ok(Expr::GetIndexedField {
//             expr: Box::new(self.expr.clone()),
//             key: ScalarValue::Utf8(Some(key.to_string())),
//         }
//         .into())
//     }
// }


#[pyclass(name = "DaskSchema", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct DaskSchema {
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

#[pyclass(name = "DaskTable", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct DaskTable {
    name: String,
    statistics: DaskStatistics,
    tableColumns: Vec<(String, DaskSqlTypeName)>,
}

#[pymethods]
impl DaskTable {
    #[new]
    pub fn new(table_name: String, row_count: f64) -> Self {
        Self {
            name: table_name,
            statistics: DaskStatistics::new(row_count),
            tableColumns: Vec::new(),
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
    m.add_wrapped(wrap_pyfunction!(getSqlNode))?;
    m.add_wrapped(wrap_pyfunction!(getRelationalAlgebra))?;
    Ok(())
}
