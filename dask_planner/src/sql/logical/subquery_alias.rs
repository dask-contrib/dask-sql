use datafusion_expr::{logical_plan::SubqueryAlias, LogicalPlan};
use pyo3::prelude::*;

use crate::sql::exceptions::py_type_err;

#[pyclass(name = "SubqueryAlias", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PySubqueryAlias {
    subquery_alias: SubqueryAlias,
}

#[pymethods]
impl PySubqueryAlias {
    /// Returns a Vec of the sort expressions
    #[pyo3(name = "getAlias")]
    pub fn alias(&self) -> PyResult<String> {
        Ok(self.subquery_alias.alias.clone())
    }
}

impl TryFrom<LogicalPlan> for PySubqueryAlias {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::SubqueryAlias(subquery_alias) => Ok(PySubqueryAlias { subquery_alias }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
