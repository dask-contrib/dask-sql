use crate::sql::table;
use crate::sql::types::rel_data_type::RelDataType;
use crate::sql::types::rel_data_type_field::RelDataTypeField;
use datafusion::logical_plan::DFField;

mod aggregate;
mod filter;
mod join;
pub mod projection;

pub use datafusion_expr::LogicalPlan;

use datafusion::prelude::Column;

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
        let proj: projection::PyProjection = self.current_node.clone().unwrap().into();
        proj.projection.input.schema().index_of_column(col).unwrap()
    }
}

#[pymethods]
impl PyLogicalPlan {
    /// LogicalPlan::Projection as PyProjection
    pub fn projection(&self) -> PyResult<projection::PyProjection> {
        let proj: projection::PyProjection = self.current_node.clone().unwrap().into();
        Ok(proj)
    }

    /// LogicalPlan::Filter as PyFilter
    pub fn filter(&self) -> PyResult<filter::PyFilter> {
        let filter: filter::PyFilter = self.current_node.clone().unwrap().into();
        Ok(filter)
    }

    /// LogicalPlan::Join as PyJoin
    pub fn join(&self) -> PyResult<join::PyJoin> {
        let join: join::PyJoin = self.current_node.clone().unwrap().into();
        Ok(join)
    }

    /// LogicalPlan::Aggregate as PyAggregate
    pub fn aggregate(&self) -> PyResult<aggregate::PyAggregate> {
        let agg: aggregate::PyAggregate = self.current_node.clone().unwrap().into();
        Ok(agg)
    }

    /// Gets the "input" for the current LogicalPlan
    pub fn get_inputs(&mut self) -> PyResult<Vec<PyLogicalPlan>> {
        let mut py_inputs: Vec<PyLogicalPlan> = Vec::new();
        for input in self.current_node().inputs() {
            py_inputs.push(input.clone().into());
        }
        Ok(py_inputs)
    }

    /// Examines the current_node and get the fields associated with it
    pub fn get_field_names(&mut self) -> PyResult<Vec<String>> {
        let mut field_names: Vec<String> = Vec::new();
        for field in self.current_node().schema().fields() {
            field_names.push(String::from(field.name()));
        }
        Ok(field_names)
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
    pub fn row_type(&self) -> RelDataType {
        let fields: &Vec<DFField> = self.original_plan.schema().fields();
        let mut rel_fields: Vec<RelDataTypeField> = Vec::new();
        for i in 0..fields.len() {
            rel_fields.push(
                RelDataTypeField::from(
                    fields[i].clone(),
                    self.original_plan.schema().as_ref().clone(),
                )
                .unwrap(),
            );
        }
        RelDataType::new(false, rel_fields)
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
