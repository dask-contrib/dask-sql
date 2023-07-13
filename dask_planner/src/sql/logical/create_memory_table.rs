use datafusion_python::{
    datafusion_expr::{
        logical_plan::{CreateMemoryTable, CreateView},
        DdlStatement,
        LogicalPlan,
    },
    sql::logical::PyLogicalPlan,
};
use pyo3::prelude::*;

use crate::sql::exceptions::py_type_err;

#[pyclass(name = "CreateMemoryTable", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyCreateMemoryTable {
    create_memory_table: Option<CreateMemoryTable>,
    create_view: Option<CreateView>,
}

#[pymethods]
impl PyCreateMemoryTable {
    #[pyo3(name = "getQualifiedName")]
    pub fn get_table_name(&self) -> PyResult<String> {
        Ok(match &self.create_memory_table {
            Some(create_memory_table) => create_memory_table.name.to_string(),
            None => match &self.create_view {
                Some(create_view) => create_view.name.to_string(),
                None => {
                    return Err(py_type_err(
                        "Encountered a non CreateMemoryTable/CreateView type in get_input",
                    ))
                }
            },
        })
    }

    #[pyo3(name = "getInput")]
    pub fn get_input(&self) -> PyResult<PyLogicalPlan> {
        Ok(match &self.create_memory_table {
            Some(create_memory_table) => PyLogicalPlan::new((*create_memory_table.input).clone()),
            None => match &self.create_view {
                Some(create_view) => PyLogicalPlan::new((*create_view.input).clone()),
                None => {
                    return Err(py_type_err(
                        "Encountered a non CreateMemoryTable/CreateView type in get_input",
                    ))
                }
            },
        })
    }

    #[pyo3(name = "getIfNotExists")]
    pub fn get_if_not_exists(&self) -> PyResult<bool> {
        Ok(match &self.create_memory_table {
            Some(create_memory_table) => create_memory_table.if_not_exists,
            None => false, // TODO: in the future we may want to set this based on dialect
        })
    }

    #[pyo3(name = "getOrReplace")]
    pub fn get_or_replace(&self) -> PyResult<bool> {
        Ok(match &self.create_memory_table {
            Some(create_memory_table) => create_memory_table.or_replace,
            None => match &self.create_view {
                Some(create_view) => create_view.or_replace,
                None => {
                    return Err(py_type_err(
                        "Encountered a non CreateMemoryTable/CreateView type in get_input",
                    ))
                }
            },
        })
    }

    #[pyo3(name = "isTable")]
    pub fn is_table(&self) -> PyResult<bool> {
        Ok(self.create_memory_table.is_some())
    }
}

impl TryFrom<LogicalPlan> for PyCreateMemoryTable {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        Ok(match logical_plan {
            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(cmt)) => PyCreateMemoryTable {
                create_memory_table: Some(cmt),
                create_view: None,
            },
            LogicalPlan::Ddl(DdlStatement::CreateView(cv)) => PyCreateMemoryTable {
                create_memory_table: None,
                create_view: Some(cv),
            },
            _ => return Err(py_type_err("unexpected plan")),
        })
    }
}
