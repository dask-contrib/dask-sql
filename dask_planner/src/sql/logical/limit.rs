use datafusion_common::ScalarValue;
use datafusion_expr::{logical_plan::Limit, Expr, LogicalPlan};
use pyo3::prelude::*;

use crate::{expression::PyExpr, sql::exceptions::py_type_err};

#[pyclass(name = "Limit", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyLimit {
    limit: Limit,
}

#[pymethods]
impl PyLimit {
    /// `OFFSET` specified in the query
    #[pyo3(name = "getSkip")]
    pub fn skip(&self) -> PyResult<PyExpr> {
        Ok(PyExpr::from(
            Expr::Literal(ScalarValue::UInt64(Some(self.limit.skip as u64))),
            Some(vec![self.limit.input.clone()]),
        ))
    }

    /// `LIMIT` specified in the query
    #[pyo3(name = "getFetch")]
    pub fn fetch(&self) -> PyResult<PyExpr> {
        Ok(PyExpr::from(
            Expr::Literal(ScalarValue::UInt64(Some(
                self.limit.fetch.unwrap_or(0) as u64
            ))),
            Some(vec![self.limit.input.clone()]),
        ))
    }
}

impl TryFrom<LogicalPlan> for PyLimit {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Limit(limit) => Ok(PyLimit { limit }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
