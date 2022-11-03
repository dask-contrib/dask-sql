use pyo3::prelude::*;

use crate::parser::DaskStatement;

#[pyclass(name = "Statement", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct PyStatement {
    pub statement: DaskStatement,
}

impl From<PyStatement> for DaskStatement {
    fn from(statement: PyStatement) -> DaskStatement {
        statement.statement
    }
}

impl From<DaskStatement> for PyStatement {
    fn from(statement: DaskStatement) -> PyStatement {
        PyStatement { statement }
    }
}

impl PyStatement {
    pub fn new(statement: DaskStatement) -> Self {
        Self { statement }
    }
}
