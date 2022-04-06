use crate::expression::PyExpr;
use crate::sql::column;
use crate::sql::table;

pub use datafusion::logical_plan::plan::{
    JoinType,
    LogicalPlan,
};

use pyo3::prelude::*;


#[pyclass(name = "LogicalPlan", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct PyLogicalPlan {
    /// The orginal LogicalPlan that was parsed by DataFusion from the input SQL
    pub(crate) original_plan: LogicalPlan,
    /// The original_plan is traversed. current_node stores the current node of this traversal
    pub(crate) current_node: Option<LogicalPlan>,
}

impl From<PyLogicalPlan> for LogicalPlan {
    fn from(logical_plan: PyLogicalPlan) -> LogicalPlan  {
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


/// Unfortunately PyO3 forces us to do this as placing these methods in the #[pymethods] version
/// of `impl PyLogicalPlan` causes issues with types not properly being mapped to Python from Rust
impl PyLogicalPlan {
    /// Getter method for the LogicalPlan, if current_node is None return original_plan.
    fn current_node(&mut self) -> LogicalPlan {
        match &self.current_node {
            Some(current) => current.clone(),
            None => {
                self.current_node = Some(self.original_plan.clone());
                self.current_node.clone().unwrap()
            },
        }
    }
}


#[pymethods]
impl PyLogicalPlan {

    /// Projection: Gets the names of the fields that should be projected
    fn get_named_projects(&mut self) -> PyResult<Vec<PyExpr>> {
        match self.current_node() {
            LogicalPlan::Projection(projection) => {
                let mut projs: Vec<PyExpr> = Vec::new();
                for expr in projection.expr {
                    projs.push(expr.clone().into());
                }
                Ok(projs)
            },
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>("current_node is not of type Projection")),
        }
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
    pub fn table(&mut self) -> PyResult<table::DaskTable> {
        match table::table_from_logical_plan(&self.current_node()) {
            Some(table) => Ok(table),
            None => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>("Unable to compute DaskTable from Datafusion LogicalPlan")),
        }
    }


    /// Gets the Relation "type" of the current node. Ex: Projection, TableScan, etc
    pub fn get_current_node_type(&mut self) -> PyResult<String> {
        match self.current_node() {
            LogicalPlan::Projection(_projection) => Ok(String::from("Projection")),
            LogicalPlan::Filter(_filter) => Ok(String::from("Filter")),
            LogicalPlan::Window(_window) => Ok(String::from("Window")),
            LogicalPlan::Aggregate(_aggregate) => Ok(String::from("Aggregate")),
            LogicalPlan::Sort(_sort) => Ok(String::from("Sort")),
            LogicalPlan::Join(_join) => Ok(String::from("Join")),
            LogicalPlan::CrossJoin(_cross_join) => Ok(String::from("CrossJoin")),
            LogicalPlan::Repartition(_repartition) => Ok(String::from("Repartition")),
            LogicalPlan::Union(_union) => Ok(String::from("Union")),
            LogicalPlan::TableScan(_table_scan) => {
                Ok(String::from("TableScan")) 
            },
            LogicalPlan::EmptyRelation(_empty_relation) => Ok(String::from("EmptyRelation")),
            LogicalPlan::Limit(_limit) => Ok(String::from("Limit")),
            LogicalPlan::CreateExternalTable(_create_external_table) => Ok(String::from("CreateExternalTable")),
            LogicalPlan::CreateMemoryTable(_create_memory_table) => Ok(String::from("CreateMemoryTable")),
            LogicalPlan::DropTable(_drop_table) => Ok(String::from("DropTable")),
            LogicalPlan::Values(_values) => Ok(String::from("Values")),
            LogicalPlan::Explain(_explain) => Ok(String::from("Explain")),
            LogicalPlan::Analyze(_analyze) => Ok(String::from("Analyze")),
            LogicalPlan::Extension(_extension) => Ok(String::from("Extension")),
        }
    }


    /// Explain plan for the full and original LogicalPlan
    pub fn explain_original(&self) -> PyResult<String> {
        Ok(format!("{}", self.original_plan.display_indent()))
    }


    /// Explain plan from the current node onward
    pub fn explain_current(&mut self) -> PyResult<String> {
        Ok(format!("{}", self.current_node().display_indent()))
    }


    /// LogicalPlan::Filter: The PyExpr, predicate, that represents the filtering condition
    pub fn getCondition(&mut self) -> PyResult<PyExpr> {
        match self.current_node() {
            LogicalPlan::Filter(filter) => {
                Ok(filter.predicate.clone().into())
            },
            _ => panic!("Something wrong here")
        }
    }

    pub fn join_conditions(&mut self) -> PyResult<Vec<(column::PyColumn, column::PyColumn)>> {
        match self.current_node() {
            LogicalPlan::Join(_join) => {
                let lhs_table_name: String = match &*_join.left {
                    LogicalPlan::TableScan(_table_scan) => {
                        let tbl: String = _table_scan.source.as_any().downcast_ref::<table::DaskTableProvider>().unwrap().table_name();
                        tbl
                    },
                    _ => panic!("lhs Expected TableScan but something else was received!")
                };

                let rhs_table_name:String = match &*_join.right {
                    LogicalPlan::TableScan(_table_scan) => {
                        let tbl: String = _table_scan.source.as_any().downcast_ref::<table::DaskTableProvider>().unwrap().table_name();
                        tbl
                    },
                    _ => panic!("rhs Expected TableScan but something else was received!")
                };

                let mut joinConditions:Vec<(column::PyColumn, column::PyColumn)> = Vec::new();
                for (mut lhs, mut rhs) in _join.on {
                    println!("lhs: {:?} rhs: {:?}", lhs, rhs);
                    lhs.relation = Some(lhs_table_name.clone());
                    rhs.relation = Some(rhs_table_name.clone());
                    joinConditions.push((lhs.into(), rhs.into()));
                }
                Ok(joinConditions)
            },
            _ => panic!("Something wrong here, lhs_on")
        }
    }

    /// Returns the type of join represented by this LogicalPlan
    pub fn getJoinType(&mut self) -> PyResult<String> {
        match &self.current_node() {
            LogicalPlan::Join(_join) => {
                match _join.join_type {
                    JoinType::Inner => Ok("INNER".to_string()),
                    JoinType::Left => Ok("LEFT".to_string()),
                    JoinType::Right => Ok("RIGHT".to_string()),
                    JoinType::Full => Ok("FULL".to_string()),
                    JoinType::Semi => Ok("SEMI".to_string()),
                    JoinType::Anti => Ok("ANTI".to_string()),
                }
            },
            _ => panic!("Current node is not of Type LogicalPlan::Join")
        }
    }
}
