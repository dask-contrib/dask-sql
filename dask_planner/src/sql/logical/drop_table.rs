use datafusion_expr::logical_plan::{DropTable, LogicalPlan};
use pyo3::prelude::*;

use crate::sql::exceptions::py_type_err;

#[pyclass(name = "DropTable", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyDropTable {
    drop_table: DropTable,
}

#[pymethods]
impl PyDropTable {
    #[pyo3(name = "getQualifiedName")]
    pub fn get_name(&self) -> PyResult<String> {
        Ok(self.drop_table.name.to_string())
    }

    #[pyo3(name = "getIfExists")]
    pub fn get_if_exists(&self) -> PyResult<bool> {
        Ok(self.drop_table.if_exists)
    }
}

impl TryFrom<LogicalPlan> for PyDropTable {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::DropTable(drop_table) => Ok(PyDropTable { drop_table }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
