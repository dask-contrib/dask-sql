use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_plan::TableScan;

use crate::sql::exceptions::py_type_err;
use pyo3::prelude::*;

#[pyclass(name = "TableScan", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyTableScan {
    pub(crate) table_scan: TableScan,
}

#[pymethods]
impl PyTableScan {
    #[pyo3(name = "getTableScanProjects")]
    fn scan_projects(&mut self) -> PyResult<Vec<String>> {
        let mut projs: Vec<String> = Vec::new();
        match &self.table_scan.projection {
            Some(e) => {
                for index in e {
                    projs.push(
                        (&*self.table_scan.source.schema())
                            .field(*index)
                            .name()
                            .clone(),
                    );
                }
            }
            None => (),
        }
        Ok(projs)
    }

    /// If the 'TableScan' contains columns that should be projected during the
    /// read return True, otherwise return False
    #[pyo3(name = "containsProjections")]
    fn contains_projections(&self) -> bool {
        match &self.table_scan.projection {
            Some(_) => true,
            None => false,
        }
    }
}

impl TryFrom<LogicalPlan> for PyTableScan {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::TableScan(table_scan) => Ok(PyTableScan { table_scan }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
