use crate::parser::Statement;

use pyo3::prelude::*;

#[pyclass(name = "Statement", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct PyStatement {
    pub statement: Statement,
}

impl From<PyStatement> for Statement {
    fn from(statement: PyStatement) -> Statement {
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
