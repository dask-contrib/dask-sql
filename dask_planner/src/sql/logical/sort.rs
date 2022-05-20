use crate::expression::PyExpr;

use datafusion::logical_expr::{logical_plan::Sort, Expr, LogicalPlan};
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
            Expr::Sort { asc, .. } => Ok(asc.clone()),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "Provided Expr {:?} is not a sort type",
                expr
            ))),
        }
    }
    /// Returns if nulls should be placed first in a sort expression
    fn is_nulls_first(&self, expr: &Expr) -> Result<bool, PyErr> {
        match &expr {
            Expr::Sort { nulls_first, .. } => Ok(nulls_first.clone()),
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
        let mut sort_exprs: Vec<PyExpr> = Vec::new();
        for expr in &self.sort.expr {
            sort_exprs.push(PyExpr::from(
                expr.clone(),
                Some(vec![self.sort.input.clone()]),
            ));
        }
        Ok(sort_exprs)
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

impl From<LogicalPlan> for PySort {
    fn from(logical_plan: LogicalPlan) -> PySort {
        match logical_plan {
            LogicalPlan::Sort(srt) => PySort { sort: srt },
            _ => panic!("something went wrong here"),
        }
    }
}
