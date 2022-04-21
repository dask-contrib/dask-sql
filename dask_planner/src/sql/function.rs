use pyo3::prelude::*;

#[pyclass(name = "DaskFunction", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct DaskFunction {
    #[allow(dead_code)]
    name: String,
}
