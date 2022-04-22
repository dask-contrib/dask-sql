use crate::expression::PyExpr;

pub use datafusion_expr::LogicalPlan;
use datafusion_expr::{logical_plan::Projection, Expr};

use pyo3::prelude::*;

#[pyclass(name = "Projection", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyProjection {
    pub(crate) projection: Projection,
}

#[pymethods]
impl PyProjection {
    #[pyo3(name = "getColumnName")]
    fn column_name(&mut self, expr: PyExpr) -> PyResult<String> {
        let mut val: String = String::from("OK");
        match expr.expr {
            Expr::Alias(expr, _alias) => match expr.as_ref() {
                Expr::Column(col) => {
                    let index = self.projection.input.schema().index_of_column(col).unwrap();
                    match self.projection.input.as_ref() {
                        LogicalPlan::Aggregate(agg) => {
                            let mut exprs = agg.group_expr.clone();
                            exprs.extend_from_slice(&agg.aggr_expr);
                            match &exprs[index] {
                                Expr::AggregateFunction { args, .. } => match &args[0] {
                                    Expr::Column(col) => {
                                        println!("AGGREGATE COLUMN IS {}", col.name);
                                        val = col.name.clone();
                                    }
                                    _ => unimplemented!(),
                                },
                                _ => unimplemented!(),
                            }
                        }
                        _ => unimplemented!(),
                    }
                }
                _ => panic!("not supported: {:?}", expr),
            },
            Expr::Column(col) => val = col.name.clone(),
            _ => panic!("Ignore for now"),
        }
        Ok(val)
    }

    /// Projection: Gets the names of the fields that should be projected
    #[pyo3(name = "getProjectedExpressions")]
    fn projected_expressions(&mut self) -> PyResult<Vec<PyExpr>> {
        let mut projs: Vec<PyExpr> = Vec::new();
        for expr in &self.projection.expr {
            projs.push(PyExpr::from(
                expr.clone(),
                Some(self.projection.input.clone()),
            ));
        }
        Ok(projs)
    }

    #[pyo3(name = "getNamedProjects")]
    fn named_projects(&mut self) -> PyResult<Vec<(String, PyExpr)>> {
        let mut named: Vec<(String, PyExpr)> = Vec::new();
        for expr in &self.projected_expressions().unwrap() {
            let name: String = self.column_name(expr.clone()).unwrap();
            named.push((name, expr.clone()));
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
