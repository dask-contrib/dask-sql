use crate::expression::PyExpr;
use crate::sql::exceptions::py_type_err;

use datafusion_common::ScalarValue;
use pyo3::prelude::*;

use datafusion_expr::{logical_plan::Offset, Expr, LogicalPlan};

#[pyclass(name = "Offset", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyOffset {
    offset: Offset,
}

#[pymethods]
impl PyOffset {
    #[pyo3(name = "getOffset")]
    pub fn offset(&self) -> PyResult<PyExpr> {
        Ok(PyExpr::from(
            Expr::Literal(ScalarValue::UInt64(Some(self.offset.offset as u64))),
            Some(vec![self.offset.input.clone()]),
        ))
    }

    #[pyo3(name = "getFetch")]
    pub fn offset_fetch(&self) -> PyResult<PyExpr> {
        // TODO: Still need to implement fetch size! For now get everything from offset on with '0'
        Ok(PyExpr::from(
            Expr::Literal(ScalarValue::UInt64(Some(0))),
            Some(vec![self.offset.input.clone()]),
        ))
    }
}

impl TryFrom<LogicalPlan> for PyOffset {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Offset(offset) => Ok(PyOffset { offset }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
