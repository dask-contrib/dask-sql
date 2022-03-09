
use std::collections::HashMap;

use pyo3::prelude::*;

use datafusion::sql::parser::{DFParser, Statement};
use sqlparser::ast::{Query, Select};

#[pyclass(name = "Statement", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub(crate) struct PyStatement {
    pub(crate) statement: Statement,
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

    #[staticmethod]
    pub fn sql(sql: &str) -> PyStatement {
        let resp = DFParser::parse_sql(sql).unwrap()[0].clone().into();
        println!("Parsed Statement from Rust: {:?}", resp);
        resp
    }
}

#[pyclass(name = "Query", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub(crate) struct PyQuery {
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


#[pyclass(name = "Select", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub(crate) struct PySelect {
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
pub(crate) struct DaskSchema {
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
}

#[pyclass(name = "DaskTable", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub(crate) struct DaskTable {
    name: String,
}

#[pyclass(name = "DaskFunction", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub(crate) struct DaskFunction {
    name: String,
}


pub(crate) fn init_module(m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(query))?;
    m.add_wrapped(wrap_pyfunction!(select))?;
    Ok(())
}
