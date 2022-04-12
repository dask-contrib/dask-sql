use datafusion::prelude::Column;

use pyo3::prelude::*;

#[pyclass(name = "Column", module = "dask_planner", subclass)]
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
    pub fn getRelation(&self) -> String {
        self.column.relation.clone().unwrap()
    }

    pub fn getName(&self) -> String {
        self.column.name.clone()
    }
}
