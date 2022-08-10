use crate::sql::exceptions::py_type_err;
use crate::sql::logical;
use datafusion_expr::logical_plan::CreateModel;
use pyo3::prelude::*;

#[pyclass(name = "CreateModel", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyCreateModel {
    pub(crate) create_model: CreateModel,
}

#[pymethods]
impl PyCreateModel {
    // #[pyo3(name = "getTableScanProjects")]
    // fn scan_projects(&mut self) -> PyResult<Vec<String>> {
    //     match &self.table_scan.projection {
    //         Some(indices) => {
    //             let schema = self.table_scan.source.schema();
    //             Ok(indices
    //                 .iter()
    //                 .map(|i| schema.field(*i).name().to_string())
    //                 .collect())
    //         }
    //         None => Ok(vec![]),
    //     }
    // }
}

impl TryFrom<logical::LogicalPlan> for PyCreateModel {
    type Error = PyErr;

    fn try_from(logical_plan: logical::LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            logical::LogicalPlan::CreateModel(create_model) => Ok(PyCreateModel { create_model }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
