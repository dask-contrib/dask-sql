use std::{any::Any, fmt, sync::Arc};

use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::{logical_plan::UserDefinedLogicalNode, Expr, LogicalPlan};
use fmt::Debug;
use pyo3::prelude::*;

use crate::sql::{exceptions::py_type_err, logical};

#[derive(Clone)]
pub struct DropSchemaPlanNode {
    pub schema: DFSchemaRef,
    pub schema_name: String,
    pub if_exists: bool,
}

impl Debug for DropSchemaPlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for DropSchemaPlanNode {
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
        // DROP SCHEMA
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DropSchema: schema_name={}", self.schema_name)
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        _inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(DropSchemaPlanNode {
            schema: Arc::new(DFSchema::empty()),
            schema_name: self.schema_name.clone(),
            if_exists: self.if_exists,
        })
    }
}

#[pyclass(name = "DropSchema", module = "dask_planner", subclass)]
pub struct PyDropSchema {
    pub(crate) drop_schema: DropSchemaPlanNode,
}

#[pymethods]
impl PyDropSchema {
    #[pyo3(name = "getSchemaName")]
    fn get_schema_name(&self) -> PyResult<String> {
        Ok(self.drop_schema.schema_name.clone())
    }

    #[pyo3(name = "getIfExists")]
    fn get_if_exists(&self) -> PyResult<bool> {
        Ok(self.drop_schema.if_exists)
    }
}

impl TryFrom<logical::LogicalPlan> for PyDropSchema {
    type Error = PyErr;

    fn try_from(logical_plan: logical::LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            logical::LogicalPlan::Extension(extension) => {
                if let Some(ext) = extension.node.as_any().downcast_ref::<DropSchemaPlanNode>() {
                    Ok(PyDropSchema {
                        drop_schema: ext.clone(),
                    })
                } else {
                    Err(py_type_err("unexpected plan"))
                }
            }
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
