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
    #[pyo3(name = "getSortExprs")]
    pub fn get_sort_exprs(&self, expr: PyExpr) -> PyResult<Vec<PyExpr>> {
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

    /// Return partition by columns from a sort expression
    #[pyo3(name = "getPartitionExprs")]
    pub fn get_partition_exprs(&self, expr: PyExpr) -> PyResult<Vec<PyExpr>> {
        match expr.expr {
            Expr::WindowFunction { partition_by, .. } => {
                let mut partition_exprs = Vec::new();
                for expr in partition_by {
                    partition_exprs
                        .push(PyExpr::from(expr.clone(), Some(self.window.input.clone())));
                }
                Ok(partition_exprs)
            }
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "Provided Expr {:?} is not a WindowFunction type",
                expr
            ))),
        }
    }

    /// Return window function name
    #[pyo3(name = "getWindowFuncName")]
    pub fn window_func_name(&self, expr: PyExpr) -> PyResult<String> {
        Ok(match expr.expr {
            Expr::WindowFunction { fun, .. } => fun.to_string(),
            _ => panic!("Encountered a non Window type in window_func_name"),
        })
    }

    /// Return input args for window function
    #[pyo3(name = "getArgs")]
    pub fn get_args(&self, expr: PyExpr) -> PyResult<Vec<PyExpr>> {
        match expr.expr {
            Expr::WindowFunction { args, .. } => {
                let mut operands = Vec::new();
                for expr in args {
                    operands.push(PyExpr::from(expr.clone(), Some(self.window.input.clone())));
                }
                Ok(operands)
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
