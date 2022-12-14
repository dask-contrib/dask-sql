use datafusion_expr::{logical_plan::Sort, LogicalPlan};
use pyo3::prelude::*;

use crate::{
    expression::{py_expr_list, PyExpr},
    sql::exceptions::py_type_err,
};

#[pyclass(name = "Sort", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PySort {
    sort: Sort,
}

#[pymethods]
impl PySort {
    /// Returns a Vec of the sort expressions
    #[pyo3(name = "getCollation")]
    pub fn sort_expressions(&self) -> PyResult<Vec<PyExpr>> {
        py_expr_list(&self.sort.input, &self.sort.expr)
    }

    #[pyo3(name = "getNumRows")]
    pub fn get_fetch_val(&self) -> PyResult<Option<usize>> {
        Ok(self.sort.fetch)
    }
}

impl TryFrom<LogicalPlan> for PySort {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Sort(sort) => Ok(PySort { sort }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
