use crate::expression::PyExpr;

pub use datafusion_expr::LogicalPlan;
use datafusion_expr::{logical_plan::Projection, Expr};

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
                let ex: Expr = *expr.clone();
                let mut py_expr: PyExpr = PyExpr::from(ex, Some(self.projection.input.clone()));
                py_expr.input_plan = local_expr.input_plan.clone();
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
        println!("Projection Input: {:?}", &self.projection.input);
        for expression in self.projection.expr.clone() {
            let mut py_expr: PyExpr = PyExpr::from(expression, Some(self.projection.input.clone()));
            py_expr.input_plan = Some(self.projection.input.clone());
            println!("Expression Input: {:?}", &py_expr.input_plan);
            for expr in self.projected_expressions(&py_expr) {
                let plan: &LogicalPlan = &*self.projection.input;
                let name: String = expr.column_name(plan.clone().into());
                println!("Named Project: {:?} - Expr: {:?}", &name, &expr);
                named.push((name, expr.clone()));
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
