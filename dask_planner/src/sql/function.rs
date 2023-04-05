use std::collections::HashMap;

use datafusion::arrow::datatypes::DataType;
use pyo3::prelude::*;

use super::types::PyDataType;

#[pyclass(name = "DaskFunction", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct DaskFunction {
    #[pyo3(get, set)]
    pub(crate) name: String,
    pub(crate) return_types: HashMap<Vec<DataType>, DataType>,
    pub(crate) aggregation: bool,
}

impl DaskFunction {
    pub fn new(
        function_name: String,
        input_types: Vec<PyDataType>,
        return_type: PyDataType,
        aggregation_bool: bool,
    ) -> Self {
        let mut func = Self {
            name: function_name,
            return_types: HashMap::new(),
            aggregation: aggregation_bool,
        };
        func.add_type_mapping(input_types, return_type);
        func
    }

    pub fn add_type_mapping(&mut self, input_types: Vec<PyDataType>, return_type: PyDataType) {
        self.return_types.insert(
            input_types.iter().map(|t| t.clone().into()).collect(),
            return_type.into(),
        );
    }
}
