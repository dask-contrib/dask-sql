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
        println!("Exprs: {:?}", &self.projection.expr);
        println!("Input: {:?}", &self.projection.input);
        println!("Schema: {:?}", &self.projection.schema);
        println!("Alias: {:?}", &self.projection.alias);
        let mut projs: Vec<PyExpr> = Vec::new();
        match &local_expr.expr {
            Expr::Alias(expr, _name) => {
                let py_expr: PyExpr =
                    PyExpr::from(*expr.clone(), Some(self.projection.input.clone()));
                projs.extend_from_slice(self.projected_expressions(&py_expr).as_slice());
            }
            _ => projs.push(local_expr.clone()),
        }
        projs
    }
}

#[pymethods]
impl PyProjection {
    #[pyo3(name = "getColumnName")]
    fn column_name(&mut self, expr: PyExpr) -> PyResult<String> {
        let mut val: String = String::from("OK");
        match expr.expr {
            Expr::Alias(expr, name) => match expr.as_ref() {
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
                                    _ => unimplemented!("projection.rs column_name is unimplemented for Expr variant: {:?}", &args[0]),
                                },
                                _ => unimplemented!("projection.rs column_name is unimplemented for Expr variant: {:?}", &exprs[index]),
                            }
                        }
                        LogicalPlan::TableScan(table_scan) => val = table_scan.table_name.clone(),
                        _ => unimplemented!("projection.rs column_name is unimplemented for LogicalPlan variant: {:?}", self.projection.input),
                    }
                }
                Expr::Cast { expr, data_type: _ } => {
                    let ex_type: Expr = *expr.clone();
                    let py_type: PyExpr =
                        PyExpr::from(ex_type, Some(self.projection.input.clone()));
                    val = self.column_name(py_type).unwrap();
                    println!("Setting col name to: {:?}", val);
                }
                _ => val = name.clone().to_ascii_uppercase(),
            },
            Expr::Column(col) => val = col.name.clone(),
            Expr::Cast { expr, data_type: _ } => {
                let ex_type: Expr = *expr;
                let py_type: PyExpr = PyExpr::from(ex_type, Some(self.projection.input.clone()));
                val = self.column_name(py_type).unwrap()
            }
            _ => {
                panic!(
                    "column_name is unimplemented for Expr variant: {:?}",
                    expr.expr
                );
            }
        }
        Ok(val)
    }

    #[pyo3(name = "getNamedProjects")]
    fn named_projects(&mut self) -> PyResult<Vec<(String, PyExpr)>> {
        let mut named: Vec<(String, PyExpr)> = Vec::new();
        for expression in self.projection.expr.clone() {
            let py_expr: PyExpr = PyExpr::from(expression, Some(self.projection.input.clone()));
            for expr in self.projected_expressions(&py_expr) {
                let name: String = self.column_name(expr.clone()).unwrap();
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
