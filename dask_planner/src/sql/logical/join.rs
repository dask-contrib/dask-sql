use datafusion_common::Column;
use datafusion_expr::{
    and,
    logical_plan::{Join, JoinType, LogicalPlan},
    BinaryExpr,
    Expr,
    Operator,
};
use pyo3::prelude::*;

use crate::{
    expression::PyExpr,
    sql::{column, exceptions::py_type_err},
};

#[pyclass(name = "Join", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyJoin {
    join: Join,
}

#[pymethods]
impl PyJoin {
    #[pyo3(name = "getCondition")]
    pub fn join_condition(&self) -> PyResult<Option<PyExpr>> {
        // equi-join filters
        let mut filters: Vec<Expr> = self
            .join
            .on
            .iter()
            .map(|(l, r)| match (l, r) {
                (Expr::Column(l), Expr::Column(r)) => {
                    Ok(Expr::Column(l.clone()).eq(Expr::Column(r.clone())))
                }
                (Expr::Column(l), Expr::Cast(cast)) => {
                    let right = Column::from_qualified_name(cast.expr.to_string());
                    Ok(Expr::Column(l.clone()).eq(Expr::Column(right)))
                }
                (Expr::Column(l), Expr::BinaryExpr(bin_expr)) => {
                    Ok(Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(Expr::Column(l.clone())),
                        Operator::Eq,
                        Box::new(Expr::BinaryExpr(bin_expr.clone())),
                    )))
                }
                _ => Err(py_type_err(format!(
                    "unsupported join condition. Left: {l} - Right: {r}"
                ))),
            })
            .collect::<Result<Vec<_>, _>>()?;

        // other filter conditions
        if let Some(filter) = &self.join.filter {
            filters.push(filter.clone());
        }

        if !filters.is_empty() {
            let root_expr = filters[1..]
                .iter()
                .fold(filters[0].clone(), |acc, expr| and(acc, expr.clone()));

            Ok(Some(PyExpr::from(
                root_expr,
                Some(vec![self.join.left.clone(), self.join.right.clone()]),
            )))
        } else {
            Ok(None)
        }
    }

    #[pyo3(name = "getJoinConditions")]
    pub fn join_conditions(&mut self) -> PyResult<Vec<(column::PyColumn, column::PyColumn)>> {
        let lhs_table_name: String = match &*self.join.left {
            LogicalPlan::TableScan(scan) => scan.table_name.clone(),
            _ => {
                return Err(py_type_err(
                    "lhs Expected TableScan but something else was received!",
                ))
            }
        };

        let rhs_table_name: String = match &*self.join.right {
            LogicalPlan::TableScan(scan) => scan.table_name.clone(),
            _ => {
                return Err(py_type_err(
                    "rhs Expected TableScan but something else was received!",
                ))
            }
        };

        let mut join_conditions: Vec<(column::PyColumn, column::PyColumn)> = Vec::new();
        for (lhs, rhs) in self.join.on.clone() {
            match (lhs, rhs) {
                (Expr::Column(mut lhs), Expr::Column(mut rhs)) => {
                    lhs.relation = Some(lhs_table_name.clone());
                    rhs.relation = Some(rhs_table_name.clone());
                    join_conditions.push((lhs.into(), rhs.into()));
                }
                _ => return Err(py_type_err("unsupported join condition")),
            }
        }
        Ok(join_conditions)
    }

    /// Returns the type of join represented by this LogicalPlan::Join instance
    #[pyo3(name = "getJoinType")]
    pub fn join_type(&mut self) -> PyResult<String> {
        match self.join.join_type {
            JoinType::Inner => Ok("INNER".to_string()),
            JoinType::Left => Ok("LEFT".to_string()),
            JoinType::Right => Ok("RIGHT".to_string()),
            JoinType::Full => Ok("FULL".to_string()),
            JoinType::LeftSemi => Ok("LEFTSEMI".to_string()),
            JoinType::LeftAnti => Ok("LEFTANTI".to_string()),
            JoinType::RightSemi => Ok("RIGHTSEMI".to_string()),
            JoinType::RightAnti => Ok("RIGHTANTI".to_string()),
        }
    }
}

impl TryFrom<LogicalPlan> for PyJoin {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Join(join) => Ok(PyJoin { join }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
