use datafusion_expr::logical_plan::Union;
pub use datafusion_expr::LogicalPlan;

use crate::sql::exceptions::py_type_err;
use pyo3::prelude::*;

#[pyclass(name = "Union", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyUnion {
    union: Union,
}

#[pymethods]
impl PyUnion {
    // TODO: Implement this method once DataFusion can
    // clearly distinguish UNION vs UNION ALL
    // #[pyo3(name = "all")]
    // pub fn all(&mut self) -> PyResult<bool> {
    //     Ok(false)
    // }
}

impl TryFrom<LogicalPlan> for PyUnion {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Union(union) => Ok(PyUnion { union }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
