use crate::expression::{py_expr_list, PyExpr};

use datafusion_expr::{logical_plan::Aggregate, logical_plan::Distinct, Expr, LogicalPlan};

use crate::sql::exceptions::py_type_err;
use pyo3::prelude::*;

#[pyclass(name = "Aggregate", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyAggregate {
    aggregate: Option<Aggregate>,
    distinct: Option<Distinct>,
}

#[pymethods]
impl PyAggregate {
    /// Determine the PyExprs that should be "Distinct-ed"
    #[pyo3(name = "getDistinctColumns")]
    pub fn distinct_columns(&self) -> PyResult<Vec<String>> {
        match &self.distinct {
            Some(e) => Ok(e.input.schema().field_names()),
            None => panic!("distinct_columns invoked for non distinct instance"),
        }
    }

    /// Returns a Vec of the group expressions
    #[pyo3(name = "getGroupSets")]
    pub fn group_expressions(&self) -> PyResult<Vec<PyExpr>> {
        match &self.aggregate {
            Some(e) => py_expr_list(&e.input, &e.group_expr),
            None => Ok(vec![]),
        }
    }

    #[pyo3(name = "getNamedAggCalls")]
    pub fn agg_expressions(&self) -> PyResult<Vec<PyExpr>> {
        match &self.aggregate {
            Some(e) => py_expr_list(&e.input, &e.aggr_expr),
            None => Ok(vec![]),
        }
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
            Expr::AggregateFunction { fun: _, args, .. } => match &self.aggregate {
                Some(e) => py_expr_list(&e.input, &args),
                None => Ok(vec![]),
            },
            _ => panic!("Encountered a non Aggregate type in agg_func_name"),
        }
    }

    #[pyo3(name = "isDistinct")]
    pub fn distinct(&self) -> PyResult<bool> {
        Ok(match self.distinct {
            Some(_) => true,
            None => false,
        })
    }
}

impl TryFrom<LogicalPlan> for PyAggregate {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Aggregate(aggregate) => Ok(PyAggregate {
                aggregate: Some(aggregate),
                distinct: None,
            }),
            LogicalPlan::Distinct(distinct) => Ok(PyAggregate {
                aggregate: None,
                distinct: Some(distinct),
            }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
