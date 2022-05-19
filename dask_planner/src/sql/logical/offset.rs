use crate::expression::PyExpr;

use datafusion::scalar::ScalarValue;
use pyo3::prelude::*;

use datafusion::logical_expr::{logical_plan::Offset, Expr, LogicalPlan};

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
            Some(self.offset.input.clone()),
        ))
    }

    #[pyo3(name = "getFetch")]
    pub fn offset_fetch(&self) -> PyResult<PyExpr> {
        // TODO: Still need to implement fetch size! For now get everything from offset on with '0'
        Ok(PyExpr::from(
            Expr::Literal(ScalarValue::UInt64(Some(0))),
            Some(self.offset.input.clone()),
        ))
    }
}

impl From<LogicalPlan> for PyOffset {
    fn from(logical_plan: LogicalPlan) -> PyOffset {
        match logical_plan {
            LogicalPlan::Offset(offset) => PyOffset { offset: offset },
            _ => panic!("Issue #501"),
        }
    }
}
