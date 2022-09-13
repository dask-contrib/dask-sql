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
            None => Err(py_type_err(
                "distinct_columns invoked for non distinct instance",
            )),
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

    /// Returns the inner Aggregate Expr(s)
    #[pyo3(name = "getNamedAggCalls")]
    pub fn agg_expressions(&self) -> PyResult<Vec<PyExpr>> {
        match &self.aggregate {
            Some(e) => py_expr_list(&e.input, &e.aggr_expr),
            None => Ok(vec![]),
        }
    }

    #[pyo3(name = "getAggregationFuncName")]
    pub fn agg_func_name(&self, expr: PyExpr) -> PyResult<String> {
        self._agg_func_name(&expr.expr)
    }

    #[pyo3(name = "getArgs")]
    pub fn aggregation_arguments(&self, expr: PyExpr) -> PyResult<Vec<PyExpr>> {
        self._aggregation_arguments(&expr.expr)
    }

    #[pyo3(name = "isAggExprDistinct")]
    pub fn distinct_agg_expr(&self, expr: PyExpr) -> PyResult<bool> {
        self._distinct_agg_expr(&expr.expr)
    }

    #[pyo3(name = "isDistinctNode")]
    pub fn distinct_node(&self) -> PyResult<bool> {
        Ok(self.distinct.is_some())
    }
}

impl PyAggregate {
    pub fn _distinct_agg_expr(&self, expr: &Expr) -> PyResult<bool> {
        match expr {
            Expr::Alias(expr, _) => self._distinct_agg_expr(expr.as_ref()),
            Expr::AggregateFunction {
                fun: _,
                args: _,
                distinct,
                filter: _,
            } => Ok(*distinct),
            _ => Err(py_type_err(
                "Encountered a non Aggregate type in distinct_agg_expr",
            )),
        }
    }

    fn _agg_func_name(&self, expr: &Expr) -> PyResult<String> {
        match expr {
            Expr::Alias(expr, _) => self._agg_func_name(expr.as_ref()),
            Expr::AggregateFunction { fun, .. } => Ok(fun.to_string()),
            Expr::AggregateUDF { fun, .. } => Ok(fun.name.clone()),
            _ => Err(py_type_err(
                "Encountered a non Aggregate type in agg_func_name",
            )),
        }
    }

    fn _aggregation_arguments(&self, expr: &Expr) -> PyResult<Vec<PyExpr>> {
        match expr {
            Expr::Alias(expr, _) => self._aggregation_arguments(expr.as_ref()),
            Expr::AggregateFunction { fun: _, args, .. }
            | Expr::AggregateUDF { fun: _, args, .. } => match &self.aggregate {
                Some(e) => py_expr_list(&e.input, &args),
                None => Ok(vec![]),
            },
            _ => Err(py_type_err(
                "Encountered a non Aggregate type in aggregation_arguments",
            )),
        }
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
