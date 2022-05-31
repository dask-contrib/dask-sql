
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
        println!("{:?}", self.union.inputs[0]);
        println!("{:?}", self.union.schema.metadata());
        println!("{:?}", self.union.alias);
        Ok(false)
    }
    
}

impl TryFrom<LogicalPlan> for PyUnion {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Union(union) => Ok(PyUnion { union }),
            _ => panic!("something went wrong here"),
        }
    }
}