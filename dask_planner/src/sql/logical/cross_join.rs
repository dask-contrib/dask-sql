use crate::sql::column;

use datafusion_expr::logical_plan::{CrossJoin, JoinType, LogicalPlan};

use pyo3::prelude::*;

#[pyclass(name = "CrossJoin", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyCrossJoin {
    cross_join: CrossJoin,
}

#[pymethods]
impl PyCrossJoin {
    // #[pyo3(name = "getJoinConditions")]
    // pub fn join_conditions(&mut self) -> PyResult<Vec<(column::PyColumn, column::PyColumn)>> {
    //     let lhs_table_name: String = match &*self.join.left {
    //         LogicalPlan::TableScan(scan) => scan.table_name.clone(),
    //         _ => panic!("lhs Expected TableScan but something else was received!"),
    //     };

    //     let rhs_table_name: String = match &*self.join.right {
    //         LogicalPlan::TableScan(scan) => scan.table_name.clone(),
    //         _ => panic!("rhs Expected TableScan but something else was received!"),
    //     };

    //     let mut join_conditions: Vec<(column::PyColumn, column::PyColumn)> = Vec::new();
    //     for (mut lhs, mut rhs) in self.join.on.clone() {
    //         lhs.relation = Some(lhs_table_name.clone());
    //         rhs.relation = Some(rhs_table_name.clone());
    //         join_conditions.push((lhs.into(), rhs.into()));
    //     }
    //     Ok(join_conditions)
    // }

    // /// Returns the type of join represented by this LogicalPlan::Join instance
    // #[pyo3(name = "getJoinType")]
    // pub fn join_type(&mut self) -> PyResult<String> {
    //     match self.join.join_type {
    //         JoinType::Inner => Ok("INNER".to_string()),
    //         JoinType::Left => Ok("LEFT".to_string()),
    //         JoinType::Right => Ok("RIGHT".to_string()),
    //         JoinType::Full => Ok("FULL".to_string()),
    //         JoinType::Semi => Ok("SEMI".to_string()),
    //         JoinType::Anti => Ok("ANTI".to_string()),
    //     }
    // }
}

impl From<LogicalPlan> for PyCrossJoin {
    fn from(logical_plan: LogicalPlan) -> PyCrossJoin {
        match logical_plan {
            LogicalPlan::CrossJoin(cross_join) => PyCrossJoin {
                cross_join: cross_join,
            },
            _ => panic!("something went wrong here"),
        }
    }
}
