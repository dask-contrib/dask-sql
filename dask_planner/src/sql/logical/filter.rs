use crate::expression::PyExpr;

pub use datafusion::logical_plan::plan::{LogicalPlan};
use datafusion::logical_plan::plan::Filter;

use pyo3::prelude::*;


#[pyclass(name = "Filter", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyFilter {
    filter: Filter
}

#[pymethods]
impl PyFilter {
    /// LogicalPlan::Filter: The PyExpr, predicate, that represents the filtering condition
    #[pyo3(name = "getCondition")]
    pub fn get_condition(&mut self) -> PyResult<PyExpr> {
        Ok(self.filter.predicate.clone().into())
    }
}


impl From<LogicalPlan> for PyFilter {
    fn from(logical_plan: LogicalPlan) -> PyFilter  {
        match logical_plan {
            LogicalPlan::Filter(filter) => {
                PyFilter {
                    filter: filter,
                }
            },
            _ => panic!("something went wrong here") ,
        }
    }
}

// impl From<LogicalPlan> for PyLogicalPlan {
//     fn from(logical_plan: LogicalPlan) -> PyLogicalPlan {
//         PyLogicalPlan {
//             original_plan: logical_plan,
//             current_node: None,
//         }
//     }
// }
