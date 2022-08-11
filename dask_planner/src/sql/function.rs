use super::types::PyDataType;
use pyo3::prelude::*;

use std::collections::HashMap;

#[pyclass(name = "DaskFunction", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct DaskFunction {
    #[pyo3(get, set)]
    pub(crate) name: String,
    pub(crate) return_types: HashMap<Vec<PyDataType>, PyDataType>,
}

impl DaskFunction {
    pub fn new(
        function_name: String,
        input_types: Vec<PyDataType>,
        return_type: PyDataType,
    ) -> Self {
        Self {
            name: function_name,
            return_types: {
                let mut map = HashMap::new();
                map.insert(input_types, return_type);
                map
            },
        }
    }
}
