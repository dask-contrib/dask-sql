use datafusion_expr::{logical_plan::Window, Expr, LogicalPlan, WindowFrame, WindowFrameBound};
use pyo3::prelude::*;

use crate::{
    expression::{py_expr_list, PyExpr},
    sql::exceptions::py_type_err,
};

#[pyclass(name = "Window", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyWindow {
    window: Window,
}

#[pyclass(name = "WindowFrame", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyWindowFrame {
    window_frame: WindowFrame,
}

#[pyclass(name = "WindowFrameBound", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyWindowFrameBound {
    frame_bound: WindowFrameBound,
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

impl From<WindowFrame> for PyWindowFrame {
    fn from(window_frame: WindowFrame) -> Self {
        PyWindowFrame { window_frame }
    }
}

impl From<WindowFrameBound> for PyWindowFrameBound {
    fn from(frame_bound: WindowFrameBound) -> Self {
        PyWindowFrameBound { frame_bound }
    }
}

#[pymethods]
impl PyWindow {
    /// Returns window expressions
    #[pyo3(name = "getGroups")]
    pub fn get_window_expr(&self) -> PyResult<Vec<PyExpr>> {
        py_expr_list(&self.window.input, &self.window.window_expr)
    }

    /// Returns order by columns in a window function expression
    #[pyo3(name = "getSortExprs")]
    pub fn get_sort_exprs(&self, expr: PyExpr) -> PyResult<Vec<PyExpr>> {
        match expr.expr {
            Expr::WindowFunction { order_by, .. } => py_expr_list(&self.window.input, &order_by),
            _ => Err(py_type_err(format!(
                "Provided Expr {:?} is not a WindowFunction type",
                expr
            ))),
        }
    }

    /// Return partition by columns in a window function expression
    #[pyo3(name = "getPartitionExprs")]
    pub fn get_partition_exprs(&self, expr: PyExpr) -> PyResult<Vec<PyExpr>> {
        match expr.expr {
            Expr::WindowFunction { partition_by, .. } => {
                py_expr_list(&self.window.input, &partition_by)
            }
            _ => Err(py_type_err(format!(
                "Provided Expr {:?} is not a WindowFunction type",
                expr
            ))),
        }
    }

    /// Return input args for window function
    #[pyo3(name = "getArgs")]
    pub fn get_args(&self, expr: PyExpr) -> PyResult<Vec<PyExpr>> {
        match expr.expr {
            Expr::WindowFunction { args, .. } => py_expr_list(&self.window.input, &args),
            _ => Err(py_type_err(format!(
                "Provided Expr {:?} is not a WindowFunction type",
                expr
            ))),
        }
    }

    /// Return window function name
    #[pyo3(name = "getWindowFuncName")]
    pub fn window_func_name(&self, expr: PyExpr) -> PyResult<String> {
        match expr.expr {
            Expr::WindowFunction { fun, .. } => Ok(fun.to_string()),
            _ => Err(py_type_err(format!(
                "Provided Expr {:?} is not a WindowFunction type",
                expr
            ))),
        }
    }

    /// Returns a Pywindow frame for a given window function expression
    #[pyo3(name = "getWindowFrame")]
    pub fn get_window_frame(&self, expr: PyExpr) -> Option<PyWindowFrame> {
        match expr.expr {
            Expr::WindowFunction { window_frame, .. } => {
                window_frame.map(|window_frame| window_frame.into())
            }
            _ => None,
        }
    }
}

#[pymethods]
impl PyWindowFrame {
    /// Returns the window frame units for the bounds
    #[pyo3(name = "getFrameUnit")]
    pub fn get_frame_units(&self) -> PyResult<String> {
        Ok(self.window_frame.units.to_string())
    }
    /// Returns starting bound
    #[pyo3(name = "getLowerBound")]
    pub fn get_lower_bound(&self) -> PyResult<PyWindowFrameBound> {
        Ok(self.window_frame.start_bound.into())
    }
    /// Returns end bound
    #[pyo3(name = "getUpperBound")]
    pub fn get_upper_bound(&self) -> PyResult<PyWindowFrameBound> {
        Ok(self.window_frame.end_bound.into())
    }
}

#[pymethods]
impl PyWindowFrameBound {
    /// Returns if the frame bound is current row
    #[pyo3(name = "isCurrentRow")]
    pub fn is_current_row(&self) -> bool {
        matches!(self.frame_bound, WindowFrameBound::CurrentRow)
    }

    /// Returns if the frame bound is preceding
    #[pyo3(name = "isPreceding")]
    pub fn is_preceding(&self) -> bool {
        matches!(self.frame_bound, WindowFrameBound::Preceding(..))
    }

    /// Returns if the frame bound is following
    #[pyo3(name = "isFollowing")]
    pub fn is_following(&self) -> bool {
        matches!(self.frame_bound, WindowFrameBound::Following(..))
    }
    /// Returns the offset of the window frame
    #[pyo3(name = "getOffset")]
    pub fn get_offset(&self) -> Option<u64> {
        match self.frame_bound {
            WindowFrameBound::Preceding(val) | WindowFrameBound::Following(val) => val,
            WindowFrameBound::CurrentRow => None,
        }
    }
    /// Returns if the frame bound is unbounded
    #[pyo3(name = "isUnbounded")]
    pub fn is_unbounded(&self) -> bool {
        match self.frame_bound {
            WindowFrameBound::Preceding(val) | WindowFrameBound::Following(val) => val.is_none(),
            WindowFrameBound::CurrentRow => false,
        }
    }
}
