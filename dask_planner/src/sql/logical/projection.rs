use crate::expression::PyExpr;

pub use datafusion::logical_plan::plan::{LogicalPlan};
use datafusion::logical_plan::{Expr};
use datafusion::logical_plan::plan::Projection;

use pyo3::prelude::*;


#[pyclass(name = "Projection", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyProjection {
    projection: Projection
}

#[pymethods]
impl PyProjection {
    /// Projection: Gets the names of the fields that should be projected
    fn get_named_projects(&mut self) -> PyResult<Vec<PyExpr>> {
        let mut projs: Vec<PyExpr> = Vec::new();
        for expr in &self.projection.expr {
            projs.push(expr.clone().into());
        }
        Ok(projs)
    }
}


impl From<LogicalPlan> for PyProjection {
    fn from(logical_plan: LogicalPlan) -> PyProjection  {
        match logical_plan {
            LogicalPlan::Projection(projection) => {
                PyProjection {
                    projection: projection
                }
            },
            _ => panic!("something went wrong here") ,
        }
    }
}
