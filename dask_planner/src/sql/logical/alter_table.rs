use std::{any::Any, fmt, sync::Arc};

use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::{
    logical_plan::{Extension, UserDefinedLogicalNode},
    Expr,
    LogicalPlan,
};
use fmt::Debug;
use pyo3::prelude::*;

use crate::sql::{exceptions::py_type_err, logical};

#[derive(Clone)]
pub struct AlterTablePlanNode {
    pub schema: DFSchemaRef,
    pub old_table_name: String,
    pub new_table_name: String,
    pub schema_name: Option<String>,
    pub if_exists: bool,
}

impl Debug for AlterTablePlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for AlterTablePlanNode {
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
        // ALTER TABLE {table_name}
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Alter Table: old_table_name: {:?}, new_table_name: {:?}, schema_name: {:?}",
            self.old_table_name, self.new_table_name, self.schema_name
        )
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        _inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(AlterTablePlanNode {
            schema: Arc::new(DFSchema::empty()),
            old_table_name: self.old_table_name.clone(),
            new_table_name: self.new_table_name.clone(),
            schema_name: self.schema_name.clone(),
            if_exists: self.if_exists,
        })
    }
}

#[pyclass(name = "AlterTable", module = "dask_planner", subclass)]
pub struct PyAlterTable {
    pub(crate) alter_table: AlterTablePlanNode,
}

#[pymethods]
impl PyAlterTable {
    #[pyo3(name = "getOldTableName")]
    fn get_old_table_name(&self) -> PyResult<String> {
        Ok(self.alter_table.old_table_name.clone())
    }

    #[pyo3(name = "getNewTableName")]
    fn get_new_table_name(&self) -> PyResult<String> {
        Ok(self.alter_table.new_table_name.clone())
    }

    #[pyo3(name = "getSchemaName")]
    fn get_schema_name(&self) -> PyResult<Option<String>> {
        Ok(self.alter_table.schema_name.clone())
    }

    #[pyo3(name = "getIfExists")]
    fn get_if_exists(&self) -> PyResult<bool> {
        Ok(self.alter_table.if_exists)
    }
}

impl TryFrom<logical::LogicalPlan> for PyAlterTable {
    type Error = PyErr;

    fn try_from(logical_plan: logical::LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Extension(Extension { node })
                if node.as_any().downcast_ref::<AlterTablePlanNode>().is_some() =>
            {
                let ext = node
                    .as_any()
                    .downcast_ref::<AlterTablePlanNode>()
                    .expect("AlterTablePlanNode");
                Ok(PyAlterTable {
                    alter_table: ext.clone(),
                })
            }
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
