use std::{
    any::Any,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

use datafusion_python::{
    datafusion_common::{DFSchema, DFSchemaRef},
    datafusion_expr::{logical_plan::UserDefinedLogicalNode, Expr, LogicalPlan},
};
use fmt::Debug;
use pyo3::prelude::*;

use crate::sql::logical::py_type_err;

#[derive(Clone, PartialEq)]
pub struct ShowModelsPlanNode {
    pub schema: DFSchemaRef,
    pub schema_name: Option<String>,
}

impl Debug for ShowModelsPlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl Hash for ShowModelsPlanNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        self.schema_name.hash(state);
    }
}

impl UserDefinedLogicalNode for ShowModelsPlanNode {
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
        // SHOW MODELS
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ShowModels")
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        _inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(ShowModelsPlanNode {
            schema: Arc::new(DFSchema::empty()),
            schema_name: self.schema_name.clone(),
        })
    }

    fn name(&self) -> &str {
        "ShowModels"
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        match other.as_any().downcast_ref::<Self>() {
            Some(o) => self == o,
            None => false,
        }
    }
}

#[pyclass(name = "ShowModels", module = "dask_sql", subclass)]
pub struct PyShowModels {
    pub(crate) show_models: ShowModelsPlanNode,
}

#[pymethods]
impl PyShowModels {
    #[pyo3(name = "getSchemaName")]
    fn get_schema_name(&self) -> PyResult<Option<String>> {
        Ok(self.show_models.schema_name.clone())
    }
}

impl TryFrom<LogicalPlan> for PyShowModels {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Extension(extension) => {
                if let Some(ext) = extension.node.as_any().downcast_ref::<ShowModelsPlanNode>() {
                    Ok(PyShowModels {
                        show_models: ext.clone(),
                    })
                } else {
                    Err(py_type_err("unexpected plan"))
                }
            }
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
