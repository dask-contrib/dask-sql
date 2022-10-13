use datafusion_expr::logical_plan::TableScan;
use pyo3::prelude::*;

use crate::sql::{exceptions::py_type_err, logical};

#[pyclass(name = "TableScan", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyTableScan {
    pub(crate) table_scan: TableScan,
}

#[pymethods]
impl PyTableScan {
    #[pyo3(name = "getTableScanProjects")]
    fn scan_projects(&mut self) -> PyResult<Vec<String>> {
        match &self.table_scan.projection {
            Some(indices) => {
                let schema = self.table_scan.source.schema();
                Ok(indices
                    .iter()
                    .map(|i| schema.field(*i).name().to_string())
                    .collect())
            }
            None => Ok(vec![]),
        }
    }

    /// If the 'TableScan' contains columns that should be projected during the
    /// read return True, otherwise return False
    #[pyo3(name = "containsProjections")]
    fn contains_projections(&self) -> bool {
        self.table_scan.projection.is_some()
    }
}

impl TryFrom<logical::LogicalPlan> for PyTableScan {
    type Error = PyErr;

    fn try_from(logical_plan: logical::LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            logical::LogicalPlan::TableScan(table_scan) => Ok(PyTableScan { table_scan }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
