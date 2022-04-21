use crate::expression::PyExpr;

use datafusion_expr::logical_plan::Filter;
pub use datafusion_expr::LogicalPlan;

use pyo3::prelude::*;

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
        Ok(self.filter.predicate.clone().into())
    }
}

impl From<LogicalPlan> for PyFilter {
    fn from(logical_plan: LogicalPlan) -> PyFilter {
        match logical_plan {
            LogicalPlan::Filter(filter) => PyFilter { filter },
            _ => panic!("something went wrong here"),
        }
    }
}
