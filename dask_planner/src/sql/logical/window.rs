use crate::expression::PyExpr;
use crate::sql::exceptions::py_type_err;
use datafusion::logical_expr::{
    logical_plan::Window, Expr, LogicalPlan, WindowFrame, WindowFrameBound,
};
use pyo3::prelude::*;

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
    /// Returns a Pywindow frame for a given windowFunction Expression
    #[pyo3(name = "getWindowFrame")]
    pub fn get_window_frame(&self, expr: PyExpr) -> Option<PyWindowFrame> {
        match expr.expr {
            Expr::WindowFunction { window_frame, .. } => match window_frame {
                Some(window_frame) => Some(window_frame.clone().into()),
                None => None,
            },
            _ => None,
        }
    }
}

#[pymethods]
impl PyWindowFrame {
    /// Returns window expressions
    #[pyo3(name = "getFrameUnit")]
    pub fn get_frame_units(&self) -> PyResult<String> {
        Ok(self.window_frame.units.to_string())
    }
    /// Returns starting bound
    #[pyo3(name = "getLowerBound")]
    pub fn get_lower_bound(&self) -> PyResult<PyWindowFrameBound> {
        Ok(self.window_frame.start_bound.clone().into())
    }
    /// Returns end bound
    #[pyo3(name = "getUpperBound")]
    pub fn get_upper_bound(&self) -> PyResult<PyWindowFrameBound> {
        Ok(self.window_frame.end_bound.clone().into())
    }
}

#[pymethods]
impl PyWindowFrameBound {
    /// Returns if the frame bound is current row
    #[pyo3(name = "isCurrentRow")]
    pub fn is_current_row(&self) -> bool {
        match self.frame_bound {
            WindowFrameBound::CurrentRow => true,
            _ => false,
        }
    }
    /// Returns if the frame bound is preceding
    #[pyo3(name = "isPreceding")]
    pub fn is_preceding(&self) -> bool {
        match self.frame_bound {
            WindowFrameBound::Preceding(..) => true,
            _ => false,
        }
    }
    /// Returns if the frame bound is preceding
    #[pyo3(name = "isFollowing")]
    pub fn is_following(&self) -> bool {
        match self.frame_bound {
            WindowFrameBound::Following(..) => true,
            _ => false,
        }
    }
    /// Returns the offset of the window frame
    #[pyo3(name = "getOffset")]
    pub fn get_offset(&self) -> PyResult<u64> {
        match self.frame_bound {
            WindowFrameBound::Preceding(val) => match val {
                Some(val) => Ok(val),
                None => Ok(0),
            },
            WindowFrameBound::CurrentRow => Ok(0),
            WindowFrameBound::Following(val) => match val {
                Some(val) => Ok(val),
                None => Ok(0),
            },
        }
    }
    /// Returns if the frame bound is preceding
    #[pyo3(name = "isUnbounded")]
    pub fn is_unbounded(&self) -> bool {
        match self.frame_bound {
            WindowFrameBound::Preceding(val) => match val {
                Some(_) => false,
                None => true,
            },
            WindowFrameBound::CurrentRow => false,
            WindowFrameBound::Following(val) => match val {
                Some(_) => false,
                None => true,
            },
        }
    }
}
