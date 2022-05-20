use crate::expression::PyExpr;

pub use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::{logical_plan::Projection, Expr};

use pyo3::prelude::*;

#[pyclass(name = "Projection", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyProjection {
    pub(crate) projection: Projection,
}

impl PyProjection {
    /// Projection: Gets the names of the fields that should be projected
    fn projected_expressions(&mut self, local_expr: &PyExpr) -> Vec<PyExpr> {
        let mut projs: Vec<PyExpr> = Vec::new();
        match &local_expr.expr {
            Expr::Alias(expr, _name) => {
                let py_expr: PyExpr =
                    PyExpr::from(*expr.clone(), Some(vec![self.projection.input.clone()]));
                projs.extend_from_slice(self.projected_expressions(&py_expr).as_slice());
            }
            _ => projs.push(local_expr.clone()),
        }
        projs
    }
}

#[pymethods]
impl PyProjection {
    #[pyo3(name = "getNamedProjects")]
    fn named_projects(&mut self) -> PyResult<Vec<(String, PyExpr)>> {
        let mut named: Vec<(String, PyExpr)> = Vec::new();
        for expression in self.projection.expr.clone() {
            let mut py_expr: PyExpr =
                PyExpr::from(expression, Some(vec![self.projection.input.clone()]));
            py_expr.input_plan = Some(vec![self.projection.input.clone()]);
            for expr in self.projected_expressions(&py_expr) {
                if let Ok(name) = expr._column_name(&*self.projection.input) {
                    named.push((name, expr.clone()));
                }
            }
        }
        Ok(named)
    }
}

impl From<LogicalPlan> for PyProjection {
    fn from(logical_plan: LogicalPlan) -> PyProjection {
        match logical_plan {
            LogicalPlan::Projection(projection) => PyProjection {
                projection: projection,
            },
            _ => panic!("something went wrong here"),
        }
    }
}
