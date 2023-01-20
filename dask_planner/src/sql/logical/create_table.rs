use std::{any::Any, fmt, sync::Arc};

use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::{logical_plan::UserDefinedLogicalNode, Expr, LogicalPlan};
use fmt::Debug;
use pyo3::prelude::*;

use crate::{
    parser::PySqlArg,
    sql::{exceptions::py_type_err, logical},
};

#[derive(Clone)]
pub struct CreateTablePlanNode {
    pub schema: DFSchemaRef,
    pub schema_name: Option<String>, // "something" in `something.table_name`
    pub table_name: String,
    pub if_not_exists: bool,
    pub or_replace: bool,
    pub with_options: Vec<(String, PySqlArg)>,
}

impl Debug for CreateTablePlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for CreateTablePlanNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        // there is no need to expose any expressions here since DataFusion would
        // not be able to do anything with expressions that are specific to
        // CREATE TABLE
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CreateTable: table_name={}", self.table_name)
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        _inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(CreateTablePlanNode {
            schema: Arc::new(DFSchema::empty()),
            schema_name: self.schema_name.clone(),
            table_name: self.table_name.clone(),
            if_not_exists: self.if_not_exists,
            or_replace: self.or_replace,
            with_options: self.with_options.clone(),
        })
    }
}

#[pyclass(name = "CreateTable", module = "dask_planner", subclass)]
pub struct PyCreateTable {
    pub(crate) create_table: CreateTablePlanNode,
}

#[pymethods]
impl PyCreateTable {
    #[pyo3(name = "getSchemaName")]
    fn get_schema_name(&self) -> PyResult<Option<String>> {
        Ok(self.create_table.schema_name.clone())
    }

    #[pyo3(name = "getTableName")]
    fn get_table_name(&self) -> PyResult<String> {
        Ok(self.create_table.table_name.clone())
    }

    #[pyo3(name = "getIfNotExists")]
    fn get_if_not_exists(&self) -> PyResult<bool> {
        Ok(self.create_table.if_not_exists)
    }

    #[pyo3(name = "getOrReplace")]
    fn get_or_replace(&self) -> PyResult<bool> {
        Ok(self.create_table.or_replace)
    }

    #[pyo3(name = "getSQLWithOptions")]
    fn sql_with_options(&self) -> PyResult<Vec<(String, PySqlArg)>> {
        Ok(self.create_table.with_options.clone())
    }
}

impl TryFrom<logical::LogicalPlan> for PyCreateTable {
    type Error = PyErr;

    fn try_from(logical_plan: logical::LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            logical::LogicalPlan::Extension(extension) => {
                if let Some(ext) = extension
                    .node
                    .as_any()
                    .downcast_ref::<CreateTablePlanNode>()
                {
                    Ok(PyCreateTable {
                        create_table: ext.clone(),
                    })
                } else {
                    Err(py_type_err("unexpected plan"))
                }
            }
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
