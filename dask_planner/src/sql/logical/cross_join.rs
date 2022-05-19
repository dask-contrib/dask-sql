use datafusion::logical_plan::{CrossJoin, LogicalPlan};

use crate::sql::exceptions::py_type_err;
use pyo3::prelude::*;

#[pyclass(name = "CrossJoin", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyCrossJoin {
    cross_join: CrossJoin,
}

impl TryFrom<LogicalPlan> for PyCrossJoin {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::CrossJoin(cross_join) => Ok(PyCrossJoin { cross_join }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
