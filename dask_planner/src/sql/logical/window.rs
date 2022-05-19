use crate::expression::PyExpr;
use crate::sql::exceptions::py_type_err;
use datafusion::logical_expr::{logical_plan::Window, Expr, LogicalPlan};
use pyo3::prelude::*;

#[pyclass(name = "Window", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyWindow {
    window: Window,
}

#[pymethods]
impl PyWindow {
    /// Returns window expressions
    #[pyo3(name = "getGroups")]
    pub fn get_window_expr(&self) -> PyResult<Vec<PyExpr>> {
        let mut window_exprs: Vec<PyExpr> = Vec::new();
        for expr in &self.window.window_expr {
            window_exprs.push(PyExpr::from(expr.clone(), Some(self.window.input.clone())));
        }
        Ok(window_exprs)
    }

    /// Returns order by columns from a sort expression
    #[pyo3(name = "getCollation")]
    pub fn get_sort_expr(&self, expr: PyExpr) -> PyResult<Vec<PyExpr>> {
        match expr.expr {
            Expr::WindowFunction { order_by, .. } => {
                let mut sort_exprs: Vec<PyExpr> = Vec::new();
                for expr in order_by {
                    sort_exprs.push(PyExpr::from(expr.clone(), Some(self.window.input.clone())));
                }
                Ok(sort_exprs)
            }
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "Provided Expr {:?} is not a WindowFunction type",
                expr
            ))),
        }
    }
}

impl TryFrom<LogicalPlan> for PyWindow {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Window(window) => Ok(PyWindow { window }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
