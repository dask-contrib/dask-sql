use crate::expression::PyExpr;

use crate::sql::exceptions::py_type_err;
use datafusion::logical_expr::{logical_plan::Sort, LogicalPlan};
use pyo3::prelude::*;

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
        let mut sort_exprs: Vec<PyExpr> = Vec::new();
        for expr in &self.sort.expr {
            sort_exprs.push(PyExpr::from(expr.clone(), Some(self.sort.input.clone())));
        }
        Ok(sort_exprs)
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
