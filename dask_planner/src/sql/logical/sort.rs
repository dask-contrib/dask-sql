use crate::expression::PyExpr;

use datafusion_expr::logical_plan::Sort;
pub use datafusion_expr::{logical_plan::LogicalPlan, Expr};

use crate::sql::exceptions::py_type_err;
use pyo3::prelude::*;

#[pyclass(name = "Sort", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PySort {
    sort: Sort,
}

impl PySort {
    /// Returns if a sort expressions denotes an ascending sort
    fn is_ascending(&self, expr: Expr) -> bool {
        match expr {
            Expr::Sort {
                expr: _,
                asc,
                nulls_first: _,
            } => asc,
            _ => panic!("Provided expression is not a sort epxression"),
        }
    }
    /// Returns if nulls should be placed first in a sort expression
    fn is_nulls_first(&self, expr: Expr) -> bool {
        match &expr {
            Expr::Sort {
                expr: _,
                asc: _,
                nulls_first,
            } => nulls_first.clone(),
            _ => panic!("Provided expression is not a sort epxression"),
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
            sort_exprs.push(PyExpr::from(expr.clone(), Some(self.sort.input.clone())));
        }
        Ok(sort_exprs)
    }

    #[pyo3(name = "getAscending")]
    pub fn get_ascending(&self) -> PyResult<Vec<bool>> {
        let mut is_ascending: Vec<bool> = Vec::new();
        for sortexpr in &self.sort.expr {
            is_ascending.push(self.is_ascending(sortexpr.clone()))
        }
        Ok(is_ascending)
    }
    #[pyo3(name = "getNullsFirst")]
    pub fn get_nulls_first(&self) -> PyResult<Vec<bool>> {
        let nulls_first: Vec<bool> = self
            .sort
            .expr
            .iter()
            .map(|sortexpr| self.is_nulls_first(sortexpr.clone()))
            .collect::<Vec<bool>>();
        Ok(nulls_first)
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
