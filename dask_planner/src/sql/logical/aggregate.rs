use crate::expression::{py_expr_list, PyExpr};
use std::sync::Arc;

use datafusion_expr::{logical_plan::Aggregate, Expr, LogicalPlan};

use crate::sql::exceptions::py_type_err;
use pyo3::prelude::*;

#[pyclass(name = "Aggregate", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyAggregate {
    aggregate: Aggregate,
}

#[pymethods]
impl PyAggregate {
    /// Returns a Vec of the group expressions
    #[pyo3(name = "getGroupSets")]
    pub fn group_expressions(&self) -> PyResult<Vec<PyExpr>> {
        py_expr_list(&self.aggregate.input, &self.aggregate.group_expr)
    }

    #[pyo3(name = "getNamedAggCalls")]
    pub fn agg_expressions(&self) -> PyResult<Vec<PyExpr>> {
        py_expr_list(&self.aggregate.input, &self.aggregate.aggr_expr)
    }

    #[pyo3(name = "getAggregationFuncName")]
    pub fn agg_func_name(&self, expr: PyExpr) -> PyResult<String> {
        Ok(match expr.expr {
            Expr::AggregateFunction { fun, .. } => fun.to_string(),
            _ => panic!("Encountered a non Aggregate type in agg_func_name"),
        })
    }

    #[pyo3(name = "getArgs")]
    pub fn aggregation_arguments(&self, expr: PyExpr) -> PyResult<Vec<PyExpr>> {
        match expr.expr {
            Expr::AggregateFunction { fun: _, args, .. } => {
                py_expr_list(&self.aggregate.input, &args)
            }
            _ => panic!("Encountered a non Aggregate type in agg_func_name"),
        }
    }

    #[pyo3(name = "isDistinct")]
    pub fn distinct(&self, expr: PyExpr) -> PyResult<bool> {
        Ok(match expr.expr {
            Expr::AggregateFunction {
                fun: _,
                args: _,
                distinct,
            } => distinct,
            _ => panic!("Encountered a non Aggregate type in agg_func_name"),
        })
    }
}

impl TryFrom<LogicalPlan> for PyAggregate {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Aggregate(aggregate) => Ok(PyAggregate { aggregate }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
