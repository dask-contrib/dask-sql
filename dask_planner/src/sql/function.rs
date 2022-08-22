use super::types::PyDataType;
use pyo3::prelude::*;

#[pyclass(name = "DaskFunction", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct DaskFunction {
    #[pyo3(get, set)]
    pub(crate) name: String,
    pub(crate) return_type: PyDataType,
}

#[pymethods]
impl DaskFunction {
    #[new]
    pub fn new(function_name: String, return_type: PyDataType) -> Self {
        Self {
            name: function_name,
            return_type,
        }
    }
}
