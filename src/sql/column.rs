use datafusion_python::datafusion_common::Column;
use pyo3::prelude::*;

#[pyclass(name = "Column", module = "dask_sql", subclass)]
#[derive(Debug, Clone)]
pub struct PyColumn {
    /// Original Column instance
    pub(crate) column: Column,
}

impl From<PyColumn> for Column {
    fn from(column: PyColumn) -> Column {
        column.column
    }
}

impl From<Column> for PyColumn {
    fn from(column: Column) -> PyColumn {
        PyColumn { column }
    }
}

#[pymethods]
impl PyColumn {
    #[pyo3(name = "getRelation")]
    pub fn relation(&self) -> String {
        self.column.relation.clone().unwrap().to_string()
    }

    #[pyo3(name = "getName")]
    pub fn name(&self) -> String {
        self.column.name.clone()
    }
}
