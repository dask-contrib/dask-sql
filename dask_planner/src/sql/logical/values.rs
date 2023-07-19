use std::sync::Arc;

use datafusion_python::datafusion_expr::{logical_plan::Values, LogicalPlan};
use pyo3::prelude::*;

use crate::{
    expression::{py_expr_list, PyExpr},
    sql::exceptions::py_type_err,
};

#[pyclass(name = "Values", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyValues {
    values: Values,
    plan: Arc<LogicalPlan>,
}

#[pymethods]
impl PyValues {
    /// Creating a model requires that a subquery be passed to the CREATE MODEL
    /// statement to be used to gather the dataset which should be used for the
    /// model. This function returns that portion of the statement.
    #[pyo3(name = "getValues")]
    fn get_values(&self) -> PyResult<Vec<Vec<PyExpr>>> {
        self.values
            .values
            .iter()
            .map(|e| py_expr_list(&self.plan, e))
            .collect()
    }
}

impl TryFrom<LogicalPlan> for PyValues {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Values(values) => Ok(PyValues {
                plan: Arc::new(LogicalPlan::Values(values.clone())),
                values,
            }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
