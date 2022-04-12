use std::collections::HashSet;
use std::sync::Arc;

use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;

use datafusion::{
    arrow::pyarrow::PyArrowConvert,
    catalog::schema::SchemaProvider,
    datasource::{TableProvider, TableType},
};

#[pyclass(name = "Database", module = "datafusion", subclass)]
pub(crate) struct PyDatabase {
    database: Arc<dyn SchemaProvider>,
}

#[pyclass(name = "Table", module = "datafusion", subclass)]
pub(crate) struct PyTable {
    table: Arc<dyn TableProvider>,
}

impl PyDatabase {
    pub fn new(database: Arc<dyn SchemaProvider>) -> Self {
        Self { database }
    }
}

impl PyTable {
    pub fn new(table: Arc<dyn TableProvider>) -> Self {
        Self { table }
    }
}

#[pymethods]
impl PyDatabase {
    fn names(&self) -> HashSet<String> {
        self.database.table_names().into_iter().collect()
    }

    fn table(&self, name: &str) -> PyResult<PyTable> {
        match self.database.table(name) {
            Some(table) => Ok(PyTable::new(table)),
            None => Err(PyKeyError::new_err(format!(
                "Table with name {} doesn't exist.",
                name
            ))),
        }
    }

    // register_table
    // deregister_table
}

#[pymethods]
impl PyTable {
    /// Get a reference to the schema for this table
    #[getter]
    fn schema(&self, py: Python) -> PyResult<PyObject> {
        self.table.schema().to_pyarrow(py)
    }

    /// Get the type of this table for metadata/catalog purposes.
    #[getter]
    fn kind(&self) -> &str {
        match self.table.table_type() {
            TableType::Base => "physical",
            TableType::View => "view",
            TableType::Temporary => "temporary",
        }
    }

    // fn scan
    // fn statistics
    // fn has_exact_statistics
    // fn supports_filter_pushdown
}
