use crate::sql::function;
use crate::sql::table;

use pyo3::prelude::*;

use std::collections::HashMap;

#[pyclass(name = "DaskSchema", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct DaskSchema {
    #[pyo3(get, set)]
    pub(crate) name: String,
    pub(crate) tables: HashMap<String, table::DaskTable>,
    #[allow(dead_code)]
    pub(crate) functions: HashMap<String, function::DaskFunction>,
}

#[pymethods]
impl DaskSchema {
    #[new]
    pub fn new(schema_name: String) -> Self {
        Self {
            name: schema_name,
            tables: HashMap::new(),
            functions: HashMap::new(),
        }
    }

    pub fn add_table(&mut self, table: table::DaskTable) {
        self.tables.insert(table.name.clone(), table);
    }
}
