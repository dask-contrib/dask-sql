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
pub struct AlterSchemaPlanNode {
    pub schema: DFSchemaRef,
    pub old_schema_name: String,
    pub new_schema_name: String,
}

impl Debug for AlterSchemaPlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for AlterSchemaPlanNode {
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
        // ALTER SCHEMA {table_name}
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Alter Schema: old_schema_name: {:?}, new_schema_name: {:?}",
            self.old_schema_name, self.new_schema_name
        )
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        _inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(AlterSchemaPlanNode {
            schema: Arc::new(DFSchema::empty()),
            old_schema_name: self.old_schema_name.clone(),
            new_schema_name: self.new_schema_name.clone(),
        })
    }
}

#[pyclass(name = "AlterSchema", module = "dask_planner", subclass)]
pub struct PyAlterSchema {
    pub(crate) alter_schema: AlterSchemaPlanNode,
}

#[pymethods]
impl PyAlterSchema {
    #[pyo3(name = "getOldSchemaName")]
    fn get_old_schema_name(&self) -> PyResult<String> {
        Ok(self.alter_schema.old_schema_name.clone())
    }

    #[pyo3(name = "getNewSchemaName")]
    fn get_new_schema_name(&self) -> PyResult<String> {
        Ok(self.alter_schema.new_schema_name.clone())
    }
}

impl TryFrom<logical::LogicalPlan> for PyAlterSchema {
    type Error = PyErr;

    fn try_from(logical_plan: logical::LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Extension(Extension { node })
                if node
                    .as_any()
                    .downcast_ref::<AlterSchemaPlanNode>()
                    .is_some() =>
            {
                let ext = node
                    .as_any()
                    .downcast_ref::<AlterSchemaPlanNode>()
                    .expect("AlterSchemaPlanNode");
                Ok(PyAlterSchema {
                    alter_schema: ext.clone(),
                })
            }
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
