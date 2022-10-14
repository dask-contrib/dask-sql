use datafusion_expr::{logical_plan::Explain, LogicalPlan};
use pyo3::prelude::*;

use crate::sql::exceptions::py_type_err;

#[pyclass(name = "Explain", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyExplain {
    explain: Explain,
}

#[pymethods]
impl PyExplain {
    /// Returns explain strings
    #[pyo3(name = "getExplainString")]
    pub fn get_explain_string(&self) -> PyResult<Vec<String>> {
        let mut string_plans: Vec<String> = Vec::new();
        for stringified_plan in &self.explain.stringified_plans {
            string_plans.push((*stringified_plan.plan).clone());
        }
        Ok(string_plans)
    }
}

impl TryFrom<LogicalPlan> for PyExplain {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Explain(explain) => Ok(PyExplain { explain }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
