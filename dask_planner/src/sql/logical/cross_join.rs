use datafusion::logical_plan::{CrossJoin, LogicalPlan};

use pyo3::prelude::*;

#[pyclass(name = "CrossJoin", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyCrossJoin {
    cross_join: CrossJoin,
}

impl From<LogicalPlan> for PyCrossJoin {
    fn from(logical_plan: LogicalPlan) -> PyCrossJoin {
        match logical_plan {
            LogicalPlan::CrossJoin(cross_join) => PyCrossJoin {
                cross_join: cross_join,
            },
            _ => panic!("something went wrong here"),
        }
    }
}
