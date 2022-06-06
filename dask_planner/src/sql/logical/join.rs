use crate::expression::PyExpr;
use crate::sql::column;

use datafusion::common::Column;
use datafusion::logical_expr::{
    col,
    logical_plan::{Join, JoinType, LogicalPlan},
    Expr, Operator,
};

use crate::sql::exceptions::py_type_err;
use pyo3::prelude::*;

#[pyclass(name = "Join", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyJoin {
    join: Join,
}

#[pymethods]
impl PyJoin {
    #[pyo3(name = "getCondition")]
    pub fn join_condition(&self) -> PyExpr {
        // TODO: This logic should be altered once https://github.com/apache/arrow-datafusion/issues/2496 is complete
        if self.join.on.len() >= 1 {
            let (left_col, right_col) = &self.join.on[0];
            let mut root_expr: Expr = Expr::BinaryExpr {
                left: Box::new(Expr::Column(left_col.clone())),
                op: Operator::Eq,
                right: Box::new(Expr::Column(right_col.clone())),
            };
            for idx in 1..self.join.on.len() {
                let (left_col, right_col) = &self.join.on[idx];
                let ex: Expr = Expr::BinaryExpr {
                    left: Box::new(Expr::Column(left_col.clone())),
                    op: Operator::Eq,
                    right: Box::new(Expr::Column(right_col.clone())),
                };

                root_expr = Expr::BinaryExpr {
                    left: Box::new(root_expr),
                    op: Operator::Eq,
                    right: Box::new(ex),
                }
            }
            PyExpr::from(
                root_expr,
                Some(vec![self.join.left.clone(), self.join.right.clone()]),
            )
        } else {
            panic!("Join Length: {}, Encountered a Join with more than a single column for the join condition. This is not currently supported
            until DataFusion makes some changes to allow for Joining logic other than just Equijoin.", self.join.on.len())
        }
    }

    #[pyo3(name = "getJoinConditions")]
    pub fn join_conditions(&mut self) -> PyResult<Vec<(column::PyColumn, column::PyColumn)>> {
        let lhs_table_name: String = match &*self.join.left {
            LogicalPlan::TableScan(scan) => scan.table_name.clone(),
            _ => panic!("lhs Expected TableScan but something else was received!"),
        };

        let rhs_table_name: String = match &*self.join.right {
            LogicalPlan::TableScan(scan) => scan.table_name.clone(),
            _ => panic!("rhs Expected TableScan but something else was received!"),
        };

        let mut join_conditions: Vec<(column::PyColumn, column::PyColumn)> = Vec::new();
        for (mut lhs, mut rhs) in self.join.on.clone() {
            lhs.relation = Some(lhs_table_name.clone());
            rhs.relation = Some(rhs_table_name.clone());
            join_conditions.push((lhs.into(), rhs.into()));
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
            JoinType::Semi => Ok("SEMI".to_string()),
            JoinType::Anti => Ok("ANTI".to_string()),
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
