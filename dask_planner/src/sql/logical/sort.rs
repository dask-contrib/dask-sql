use crate::expression::{py_expr_list, PyExpr};

use crate::sql::exceptions::py_type_err;
use datafusion_expr::{logical_plan::Sort, Expr, LogicalPlan};
use pyo3::prelude::*;

#[pyclass(name = "Sort", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PySort {
    sort: Sort,
}

impl PySort {
    /// Returns if a sort expressions denotes an ascending sort
    fn is_ascending(&self, expr: &Expr) -> Result<bool, PyErr> {
        match expr {
            Expr::Sort { asc, .. } => Ok(*asc),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "Provided Expr {:?} is not a sort type",
                expr
            ))),
        }
    }
    /// Returns if nulls should be placed first in a sort expression
    fn is_nulls_first(&self, expr: &Expr) -> Result<bool, PyErr> {
        match &expr {
            Expr::Sort { nulls_first, .. } => Ok(*nulls_first),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "Provided Expr {:?} is not a sort type",
                expr
            ))),
        }
    }
}
#[pymethods]
impl PySort {
    /// Returns a Vec of the sort expressions
    #[pyo3(name = "getCollation")]
    pub fn sort_expressions(&self) -> PyResult<Vec<PyExpr>> {
        py_expr_list(self.sort.input.clone(), &self.sort.expr)
    }

    #[pyo3(name = "getAscending")]
    pub fn get_ascending(&self) -> PyResult<Vec<bool>> {
        self.sort
            .expr
            .iter()
            .map(|sortexpr| self.is_ascending(sortexpr))
            .collect::<Result<Vec<_>, _>>()
    }
    #[pyo3(name = "getNullsFirst")]
    pub fn get_nulls_first(&self) -> PyResult<Vec<bool>> {
        self.sort
            .expr
            .iter()
            .map(|sortexpr| self.is_nulls_first(sortexpr))
            .collect::<Result<Vec<_>, _>>()
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
