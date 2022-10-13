use datafusion_expr::{logical_plan::Filter, LogicalPlan};
use pyo3::prelude::*;

use crate::{expression::PyExpr, sql::exceptions::py_type_err};

#[pyclass(name = "Filter", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyFilter {
    filter: Filter,
}

#[pymethods]
impl PyFilter {
    /// LogicalPlan::Filter: The PyExpr, predicate, that represents the filtering condition
    #[pyo3(name = "getCondition")]
    pub fn get_condition(&mut self) -> PyResult<PyExpr> {
        Ok(PyExpr::from(
            self.filter.predicate.clone(),
            Some(vec![self.filter.input.clone()]),
        ))
    }
}

impl TryFrom<LogicalPlan> for PyFilter {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Filter(filter) => Ok(PyFilter { filter }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
