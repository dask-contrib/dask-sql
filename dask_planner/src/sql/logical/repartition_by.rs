use crate::sql::logical;
use crate::{expression::PyExpr, sql::exceptions::py_type_err};
use datafusion_expr::logical_plan::{Partitioning, Repartition};
use pyo3::prelude::*;

use datafusion_expr::LogicalPlan;

#[pyclass(name = "RepartitionBy", module = "dask_planner", subclass)]
pub struct PyRepartitionBy {
    pub(crate) repartition: Repartition,
}

#[pymethods]
impl PyRepartitionBy {
    #[pyo3(name = "getSelectQuery")]
    fn get_select_query(&self) -> PyResult<logical::PyLogicalPlan> {
        let log_plan = &*(self.repartition.input).clone();
        Ok(log_plan.clone().into())
    }

    #[pyo3(name = "getDistributeList")]
    fn get_distribute_list(&self) -> PyResult<Vec<PyExpr>> {
        match &self.repartition.partitioning_scheme {
            Partitioning::DistributeBy(distribute_list) => Ok(distribute_list
                .iter()
                .map(|e| PyExpr::from(e.clone(), Some(vec![self.repartition.input.clone()])))
                .collect()),
            _ => Err(py_type_err("unexpected repartition strategy")),
        }
    }
}

impl TryFrom<LogicalPlan> for PyRepartitionBy {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Repartition(repartition) => Ok(PyRepartitionBy { repartition }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
