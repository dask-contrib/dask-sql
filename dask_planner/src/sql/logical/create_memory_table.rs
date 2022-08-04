use crate::sql::exceptions::py_type_err;
use crate::sql::logical::PyLogicalPlan;
use datafusion_expr::{logical_plan::CreateMemoryTable, logical_plan::CreateView, LogicalPlan};
use pyo3::prelude::*;

#[pyclass(name = "CreateMemoryTable", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyCreateMemoryTable {
    create_memory_table: Option<CreateMemoryTable>,
    create_view: Option<CreateView>,
}

#[pymethods]
impl PyCreateMemoryTable {
    #[pyo3(name = "getName")]
    pub fn get_name(&self) -> PyResult<String> {
        match &self.create_memory_table {
            Some(create_memory_table) => Ok(format!("{}", create_memory_table.name)),
            None => match &self.create_view {
                Some(create_view) => Ok(format!("{}", create_view.name)),
                None => Err(py_type_err(
                    "Encountered a non CreateMemoryTable/CreateView type in get_input",
                )),
            },
        }
    }

    #[pyo3(name = "getInput")]
    pub fn get_input(&self) -> PyResult<PyLogicalPlan> {
        match &self.create_memory_table {
            Some(create_memory_table) => Ok(PyLogicalPlan {
                original_plan: (*create_memory_table.input).clone(),
                current_node: None,
            }),
            None => match &self.create_view {
                Some(create_view) => Ok(PyLogicalPlan {
                    original_plan: (*create_view.input).clone(),
                    current_node: None,
                }),
                None => Err(py_type_err(
                    "Encountered a non CreateMemoryTable/CreateView type in get_input",
                )),
            },
        }
    }

    #[pyo3(name = "getIfNotExists")]
    pub fn get_if_not_exists(&self) -> PyResult<bool> {
        match &self.create_memory_table {
            Some(create_memory_table) => Ok(create_memory_table.if_not_exists),
            None => Ok(false), // TODO: in the future we may want to set this based on dialect
        }
    }

    #[pyo3(name = "getOrReplace")]
    pub fn get_or_replace(&self) -> PyResult<bool> {
        match &self.create_memory_table {
            Some(create_memory_table) => Ok(create_memory_table.or_replace),
            None => match &self.create_view {
                Some(create_view) => Ok(create_view.or_replace),
                None => Err(py_type_err(
                    "Encountered a non CreateMemoryTable/CreateView type in get_input",
                )),
            },
        }
    }
}

impl TryFrom<LogicalPlan> for PyCreateMemoryTable {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::CreateMemoryTable(create_memory_table) => Ok(PyCreateMemoryTable {
                create_memory_table: Some(create_memory_table),
                create_view: None,
            }),
            LogicalPlan::CreateView(create_view) => Ok(PyCreateMemoryTable {
                create_memory_table: None,
                create_view: Some(create_view),
            }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
