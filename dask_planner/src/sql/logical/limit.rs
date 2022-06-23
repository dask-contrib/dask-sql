use crate::expression::PyExpr;
use crate::sql::exceptions::py_type_err;

use datafusion_common::ScalarValue;
use pyo3::prelude::*;

use datafusion_expr::{logical_plan::Limit, Expr, LogicalPlan};

#[pyclass(name = "Limit", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyLimit {
    limit: Limit,
}

#[pymethods]
impl PyLimit {
    #[pyo3(name = "getLimitN")]
    pub fn limit_n(&self) -> PyResult<PyExpr> {
        Ok(PyExpr::from(
            Expr::Literal(ScalarValue::UInt64(Some(self.limit.n.try_into().unwrap()))),
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
