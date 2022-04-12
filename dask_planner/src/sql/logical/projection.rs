use crate::expression::PyExpr;

pub use datafusion::logical_plan::plan::LogicalPlan;
use datafusion::logical_plan::plan::Projection;

use datafusion::logical_plan::Expr;

use pyo3::prelude::*;

#[pyclass(name = "Projection", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyProjection {
    pub(crate) projection: Projection,
}

#[pymethods]
impl PyProjection {
    #[pyo3(name = "getColumnName")]
    fn named_projects(&mut self, expr: PyExpr) -> PyResult<String> {
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
                _ => println!("not supported: {:?}", expr),
            },
            _ => println!("Ignore for now"),
        }
        Ok(val)
    }

    /// Projection: Gets the names of the fields that should be projected
    #[pyo3(name = "getProjectedExpressions")]
    fn projected_expressions(&mut self) -> PyResult<Vec<PyExpr>> {
        let mut projs: Vec<PyExpr> = Vec::new();
        for expr in &self.projection.expr {
            projs.push(expr.clone().into());
        }
        Ok(projs)
    }
    // fn named_projects(&mut self) {
    //     for expr in &self.projection.expr {
    //         match expr {
    //             Expr::Alias(expr, alias) => {
    //                 match expr.as_ref() {
    //                     Expr::Column(col) => {
    //                         let index = self.projection.input.schema().index_of_column(&col).unwrap();
    //                         println!("projection column '{}' maps to input column {}", col.to_string(), index);
    //                         let f: &DFField = self.projection.input.schema().field(index);
    //                         println!("Field: {:?}", f);

    //                         match self.projection.input.as_ref() {
    //                             LogicalPlan::Aggregate(agg) => {
    //                                 let mut exprs = agg.group_expr.clone();
    //                                 exprs.extend_from_slice(&agg.aggr_expr);
    //                                 match &exprs[index] {
    //                                     Expr::AggregateFunction { args, .. } => {
    //                                         match &args[0] {
    //                                             Expr::Column(col) => {
    //                                                 println!("AGGREGATE COLUMN IS {}", col.name);
    //                                             },
    //                                             _ => unimplemented!()
    //                                         }
    //                                     },
    //                                     _ => unimplemented!()
    //                                 }
    //                             },
    //                             _ => unimplemented!()
    //                         }
    //                     }
    //                     _ => unimplemented!()
    //                 }
    //             },
    //             _ => println!("not supported: {:?}", expr)
    //         }
    //     }
    // }

    // fn named_projects(&mut self) {
    //     match self.projection.input.as_ref() {
    //         LogicalPlan::Aggregate(agg) => {
    //             match &agg.aggr_expr[0] {
    //                 Expr::AggregateFunction { args, .. } => {
    //                     match &args[0] {
    //                         Expr::Column(col) => {
    //                             println!("AGGREGATE COLUMN IS {}", col.name);
    //                         },
    //                         _ => unimplemented!()
    //                     }
    //                 },
    //                 _ => println!("ignore for now")
    //             }
    //         },
    //         _ => unimplemented!()
    //     }
    // }
}

impl From<LogicalPlan> for PyProjection {
    fn from(logical_plan: LogicalPlan) -> PyProjection {
        match logical_plan {
            LogicalPlan::Projection(projection) => PyProjection { projection },
            _ => panic!("something went wrong here"),
        }
    }
}
