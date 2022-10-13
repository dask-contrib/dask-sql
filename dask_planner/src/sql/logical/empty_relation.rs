use datafusion_expr::logical_plan::{EmptyRelation, LogicalPlan};
use pyo3::prelude::*;

use crate::sql::exceptions::py_type_err;

#[pyclass(name = "EmptyRelation", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyEmptyRelation {
    empty_relation: EmptyRelation,
}

impl TryFrom<LogicalPlan> for PyEmptyRelation {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::EmptyRelation(empty_relation) => Ok(PyEmptyRelation { empty_relation }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}

#[pymethods]
impl PyEmptyRelation {
    /// Even though a relation results in an "empty" table column names
    /// will still be projected and must be captured in order to present
    /// the expected output to the user. This logic captures the names
    /// of those columns and returns them to the Python logic where
    /// there are rendered to the user
    #[pyo3(name = "emptyColumnNames")]
    pub fn empty_column_names(&self) -> PyResult<Vec<String>> {
        Ok(self.empty_relation.schema.field_names())
    }
}
