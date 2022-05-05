use crate::expression::PyExpr;

use datafusion_expr::logical_plan::Sort;
pub use datafusion_expr::{logical_plan::LogicalPlan, Expr};
use pyo3::prelude::*;

#[pyclass(name = "Sort", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PySort {
    sort: Sort,
}

#[pymethods]
impl PySort {
    /// Returns a Vec of the sort expressions
    #[pyo3(name = "getCollation")]
    pub fn sort_expressions(&self) -> PyResult<Vec<PyExpr>> {
        let mut sort_exprs: Vec<PyExpr> = Vec::new();
        for expr in &self.sort.expr {
            println!("The expr seen is {}", expr);
            sort_exprs.push(PyExpr::from(expr.clone(), Some(self.sort.input.clone())));
        }
        Ok(sort_exprs)
    }
    #[pyo3(name = "getColumnName")]
    pub fn get_column(&self, expr: PyExpr) -> String {
        match &expr.expr {
            Expr::Sort {
                expr,
                asc,
                nulls_first,
            } => {
                println!("expr {}", expr);
                println!("asc {}", asc);
                println!("nulls_first {}", nulls_first);
                match expr.as_ref() {
                    Expr::Column(col) => {
                        println!("Is a column {}", col);
                        println!("col_name {}", col.name);
                        col.name.clone()
                    }
                    _ => panic!("Sort Expression expr not of column type!"),
                }
            }
            _ => panic!("Provided expression is not a sort expression"),
        }
    }
    #[pyo3(name = "isAscending")]
    pub fn is_ascending(&self, expr: PyExpr) -> bool {
        match &expr.expr {
            Expr::Sort {
                expr: _,
                asc,
                nulls_first: _,
            } => asc.clone(),
            _ => panic!("Provided expression is not a sort expression"),
        }
    }
    #[pyo3(name = "isNullsFirst")]
    pub fn is_nulls_first(&self, expr: PyExpr) -> bool {
        match &expr.expr {
            Expr::Sort {
                expr: _,
                asc: _,
                nulls_first,
            } => nulls_first.clone(),
            _ => panic!("Provided expression is not a sort expression"),
        }
    }
}

impl From<LogicalPlan> for PySort {
    fn from(logical_plan: LogicalPlan) -> PySort {
        match logical_plan {
            LogicalPlan::Sort(srt) => PySort { sort: srt },
            _ => panic!("something went wrong here"),
        }
    }
}
