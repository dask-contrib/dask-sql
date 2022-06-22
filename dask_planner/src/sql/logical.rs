use crate::sql::table;
use crate::sql::types::rel_data_type::RelDataType;
use crate::sql::types::rel_data_type_field::RelDataTypeField;

mod aggregate;
mod cross_join;
mod explain;
mod filter;
mod join;
mod limit;
mod offset;
mod projection;
mod sort;
mod table_scan;
mod union;

use datafusion_common::{Column, DFSchemaRef, DataFusionError, Result};
use datafusion_expr::LogicalPlan;

use crate::sql::exceptions::py_type_err;
use pyo3::prelude::*;

#[pyclass(name = "LogicalPlan", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct PyLogicalPlan {
    /// The orginal LogicalPlan that was parsed by DataFusion from the input SQL
    pub(crate) original_plan: LogicalPlan,
    /// The original_plan is traversed. current_node stores the current node of this traversal
    pub(crate) current_node: Option<LogicalPlan>,
}

/// Unfortunately PyO3 forces us to do this as placing these methods in the #[pymethods] version
/// of `impl PyLogicalPlan` causes issues with types not properly being mapped to Python from Rust
impl PyLogicalPlan {
    /// Getter method for the LogicalPlan, if current_node is None return original_plan.
    pub(crate) fn current_node(&mut self) -> LogicalPlan {
        match &self.current_node {
            Some(current) => current.clone(),
            None => {
                self.current_node = Some(self.original_plan.clone());
                self.current_node.clone().unwrap()
            }
        }
    }

    /// Gets the index of the column from the input schema
    pub(crate) fn get_index(&mut self, col: &Column) -> usize {
        let proj: projection::PyProjection = self.projection().unwrap();
        proj.projection.input.schema().index_of_column(col).unwrap()
    }
}

/// Convert a LogicalPlan to a Python equivalent type
fn to_py_plan<T: TryFrom<LogicalPlan, Error = PyErr>>(
    current_node: Option<&LogicalPlan>,
) -> PyResult<T> {
    match current_node {
        Some(plan) => plan.clone().try_into(),
        _ => Err(py_type_err("current_node was None")),
    }
}

#[pymethods]
impl PyLogicalPlan {
    /// LogicalPlan::Aggregate as PyAggregate
    pub fn aggregate(&self) -> PyResult<aggregate::PyAggregate> {
        to_py_plan(self.current_node.as_ref())
    }

    /// LogicalPlan::CrossJoin as PyCrossJoin
    pub fn cross_join(&self) -> PyResult<cross_join::PyCrossJoin> {
        to_py_plan(self.current_node.as_ref())
    }

    /// LogicalPlan::Explain as PyExplain
    pub fn explain(&self) -> PyResult<explain::PyExplain> {
        to_py_plan(self.current_node.as_ref())
    }

    /// LogicalPlan::Union as PyUnion
    pub fn union(&self) -> PyResult<union::PyUnion> {
        to_py_plan(self.current_node.as_ref())
    }

    /// LogicalPlan::Filter as PyFilter
    pub fn filter(&self) -> PyResult<filter::PyFilter> {
        to_py_plan(self.current_node.as_ref())
    }

    /// LogicalPlan::Join as PyJoin
    pub fn join(&self) -> PyResult<join::PyJoin> {
        to_py_plan(self.current_node.as_ref())
    }

    /// LogicalPlan::Limit as PyLimit
    pub fn limit(&self) -> PyResult<limit::PyLimit> {
        to_py_plan(self.current_node.as_ref())
    }

    /// LogicalPlan::Offset as PyOffset
    pub fn offset(&self) -> PyResult<offset::PyOffset> {
        to_py_plan(self.current_node.as_ref())
    }

    /// LogicalPlan::Projection as PyProjection
    pub fn projection(&self) -> PyResult<projection::PyProjection> {
        to_py_plan(self.current_node.as_ref())
    }

    /// LogicalPlan::Sort as PySort
    pub fn sort(&self) -> PyResult<sort::PySort> {
        to_py_plan(self.current_node.as_ref())
    }

    /// LogicalPlan::TableScan as PyTableScan
    pub fn table_scan(&self) -> PyResult<table_scan::PyTableScan> {
        to_py_plan(self.current_node.as_ref())
    }

    /// Gets the "input" for the current LogicalPlan
    pub fn get_inputs(&mut self) -> PyResult<Vec<PyLogicalPlan>> {
        let mut py_inputs: Vec<PyLogicalPlan> = Vec::new();
        for input in self.current_node().inputs() {
            py_inputs.push(input.clone().into());
        }
        Ok(py_inputs)
    }

    /// If the LogicalPlan represents access to a Table that instance is returned
    /// otherwise None is returned
    #[pyo3(name = "getTable")]
    pub fn table(&mut self) -> PyResult<table::DaskTable> {
        match table::table_from_logical_plan(&self.current_node()) {
            Some(table) => Ok(table),
            None => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Unable to compute DaskTable from DataFusion LogicalPlan",
            )),
        }
    }

    #[pyo3(name = "getCurrentNodeSchemaName")]
    pub fn get_current_node_schema_name(&self) -> PyResult<&str> {
        match &self.current_node {
            Some(e) => {
                let _sch: &DFSchemaRef = e.schema();
                //TODO: Where can I actually get this in the context of the running query?
                Ok("root")
            }
            None => Err(py_type_err(DataFusionError::Plan(format!(
                "Current schema not found. Defaulting to {:?}",
                "root"
            )))),
        }
    }

    #[pyo3(name = "getCurrentNodeTableName")]
    pub fn get_current_node_table_name(&mut self) -> PyResult<String> {
        match self.table() {
            Ok(dask_table) => Ok(dask_table.name),
            Err(_e) => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Unable to determine current node table name",
            )),
        }
    }

    /// Gets the Relation "type" of the current node. Ex: Projection, TableScan, etc
    pub fn get_current_node_type(&mut self) -> PyResult<&str> {
        Ok(match self.current_node() {
            LogicalPlan::Projection(_projection) => "Projection",
            LogicalPlan::Filter(_filter) => "Filter",
            LogicalPlan::Window(_window) => "Window",
            LogicalPlan::Aggregate(_aggregate) => "Aggregate",
            LogicalPlan::Sort(_sort) => "Sort",
            LogicalPlan::Join(_join) => "Join",
            LogicalPlan::CrossJoin(_cross_join) => "CrossJoin",
            LogicalPlan::Repartition(_repartition) => "Repartition",
            LogicalPlan::Union(_union) => "Union",
            LogicalPlan::TableScan(_table_scan) => "TableScan",
            LogicalPlan::EmptyRelation(_empty_relation) => "EmptyRelation",
            LogicalPlan::Limit(_limit) => "Limit",
            LogicalPlan::Offset(_offset) => "Offset",
            LogicalPlan::CreateExternalTable(_create_external_table) => "CreateExternalTable",
            LogicalPlan::CreateMemoryTable(_create_memory_table) => "CreateMemoryTable",
            LogicalPlan::DropTable(_drop_table) => "DropTable",
            LogicalPlan::Values(_values) => "Values",
            LogicalPlan::Explain(_explain) => "Explain",
            LogicalPlan::Analyze(_analyze) => "Analyze",
            LogicalPlan::Extension(_extension) => "Extension",
            LogicalPlan::Subquery(_sub_query) => "Subquery",
            LogicalPlan::SubqueryAlias(_sqalias) => "SubqueryAlias",
            LogicalPlan::CreateCatalogSchema(_create) => "CreateCatalogSchema",
            LogicalPlan::CreateCatalog(_create_catalog) => "CreateCatalog",
            LogicalPlan::CreateView(_create_view) => "CreateView",
        })
    }

    /// Explain plan for the full and original LogicalPlan
    pub fn explain_original(&self) -> PyResult<String> {
        Ok(format!("{}", self.original_plan.display_indent()))
    }

    /// Explain plan from the current node onward
    pub fn explain_current(&mut self) -> PyResult<String> {
        Ok(format!("{}", self.current_node().display_indent()))
    }

    #[pyo3(name = "getRowType")]
    pub fn row_type(&self) -> PyResult<RelDataType> {
        let schema = self.original_plan.schema();
        let rel_fields: Vec<RelDataTypeField> = schema
            .fields()
            .iter()
            .map(|f| RelDataTypeField::from(f, schema.as_ref()))
            .collect::<Result<Vec<_>>>()
            .map_err(|e| py_type_err(e))?;
        Ok(RelDataType::new(false, rel_fields))
    }
}

impl From<PyLogicalPlan> for LogicalPlan {
    fn from(logical_plan: PyLogicalPlan) -> LogicalPlan {
        logical_plan.original_plan
    }
}

impl From<LogicalPlan> for PyLogicalPlan {
    fn from(logical_plan: LogicalPlan) -> PyLogicalPlan {
        PyLogicalPlan {
            original_plan: logical_plan,
            current_node: None,
        }
    }
}
