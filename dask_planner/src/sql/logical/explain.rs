use datafusion::logical_expr::{logical_plan::Explain, LogicalPlan};
use pyo3::prelude::*;

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
            string_plans.push((&*stringified_plan.plan).clone());
        }
        Ok(string_plans)
    }
}

impl From<LogicalPlan> for PyExplain {
    fn from(logical_plan: LogicalPlan) -> PyExplain {
        match logical_plan {
            LogicalPlan::Explain(expln) => PyExplain { explain: expln },
            _ => panic!("something went wrong here"),
        }
    }
}
