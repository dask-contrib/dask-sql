use datafusion_python::{
    datafusion_expr::{logical_plan::Projection, Expr, LogicalPlan},
    expr::{projection::PyProjection as ADPPyProjection, PyExpr},
};
use pyo3::prelude::*;

use crate::sql::{exceptions::py_type_err, logical::utils::column_name};

#[pyclass(name = "Projection", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyProjection {
    pub(crate) projection: Projection,
}

#[pymethods]
impl PyProjection {
    #[pyo3(name = "getNamedProjects")]
    fn named_projects(&mut self) -> PyResult<Vec<(String, PyExpr)>> {
        let mut named: Vec<(String, PyExpr)> = Vec::new();
        for expression in self.projection.expr.clone() {
            let py_expr: PyExpr = PyExpr::from(expression);
            for expr in ADPPyProjection::projected_expressions(&py_expr) {
                match expr.expr {
                    Expr::Alias(ex, name) => named.push((name.to_string(), PyExpr::from(*ex))),
                    _ => {
                        if let Ok(name) = column_name(&expr.expr, &self.projection.input) {
                            named.push((name, expr.clone()));
                        }
                    }
                }
            }
        }
        Ok(named)
    }
}

impl TryFrom<LogicalPlan> for PyProjection {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Projection(projection) => Ok(PyProjection { projection }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
