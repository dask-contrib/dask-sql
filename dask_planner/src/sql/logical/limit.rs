use crate::expression::PyExpr;

use datafusion::scalar::ScalarValue;
use pyo3::prelude::*;

use datafusion::logical_expr::logical_plan::Limit;
use datafusion::logical_expr::{Expr, LogicalPlan};

#[pyclass(name = "Limit", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyLimit {
    limit: Limit,
}

#[pymethods]
impl PyLimit {
    #[pyo3(name = "getOffset")]
    pub fn limit_offset(&self) -> PyResult<PyExpr> {
        // TODO: Waiting on DataFusion issue: https://github.com/apache/arrow-datafusion/issues/2377
        Ok(PyExpr::from(
            Expr::Literal(ScalarValue::UInt64(Some(0))),
            Some(self.limit.input.clone()),
        ))
    }

    #[pyo3(name = "getFetch")]
    pub fn limit_n(&self) -> PyResult<PyExpr> {
        Ok(PyExpr::from(
            Expr::Literal(ScalarValue::UInt64(Some(self.limit.n.try_into().unwrap()))),
            Some(self.limit.input.clone()),
        ))
    }
}

impl From<LogicalPlan> for PyLimit {
    fn from(logical_plan: LogicalPlan) -> PyLimit {
        match logical_plan {
            LogicalPlan::Limit(limit) => PyLimit { limit: limit },
            _ => panic!("something went wrong here"),
        }
    }
}
