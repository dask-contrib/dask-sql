use std::collections::HashMap;

use ::std::sync::{Arc, Mutex};
use pyo3::prelude::*;

use super::types::PyDataType;
use crate::sql::{function::DaskFunction, table};

#[pyclass(name = "DaskSchema", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct DaskSchema {
    #[pyo3(get, set)]
    pub(crate) name: String,
    pub(crate) tables: HashMap<String, table::DaskTable>,
    pub(crate) functions: HashMap<String, Arc<Mutex<DaskFunction>>>,
}

#[pymethods]
impl DaskSchema {
    #[new]
    pub fn new(schema_name: &str) -> Self {
        Self {
            name: schema_name.to_owned(),
            tables: HashMap::new(),
            functions: HashMap::new(),
        }
    }

    pub fn add_table(&mut self, table: table::DaskTable) {
        self.tables.insert(table.table_name.clone(), table);
    }

    pub fn add_or_overload_function(
        &mut self,
        name: String,
        input_types: Vec<PyDataType>,
        return_type: PyDataType,
        aggregation: bool,
    ) {
        self.functions
            .entry(name.clone())
            .and_modify(|e| {
                (*e).lock()
                    .unwrap()
                    .add_type_mapping(input_types.clone(), return_type.clone());
            })
            .or_insert_with(|| {
                Arc::new(Mutex::new(DaskFunction::new(
                    name,
                    input_types,
                    return_type,
                    aggregation,
                )))
            });
    }
}
