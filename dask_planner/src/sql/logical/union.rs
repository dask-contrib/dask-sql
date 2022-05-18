
use datafusion::logical_expr::logical_plan::Union;
pub use datafusion::logical_expr::{LogicalPlan};

use pyo3::prelude::*;

#[pyclass(name = "Union", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyUnion {
    union: Union,
}

#[pymethods]
impl PyUnion {
    #[pyo3(name = "all")]
    pub fn all(&mut self) -> PyResult<bool> {
        Ok(false)
    }
    
}

impl From<LogicalPlan> for PyUnion {
    fn from(logical_plan: LogicalPlan) -> PyUnion {
        match logical_plan {
            LogicalPlan::Union(union) => PyUnion { union },
            _ => panic!("something went wrong here"),
        }
    }
}