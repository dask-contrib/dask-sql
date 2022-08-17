use crate::sql::exceptions::py_type_err;
use crate::sql::logical;
use pyo3::prelude::*;

use datafusion_expr::logical_plan::UserDefinedLogicalNode;
use datafusion_expr::{Expr, LogicalPlan};

use fmt::Debug;
use std::{any::Any, fmt, sync::Arc};

use datafusion_common::{DFSchema, DFSchemaRef};

#[derive(Clone)]
pub struct ShowSchemasPlanNode {
    pub schema: DFSchemaRef,
    pub like: Option<String>,
}

impl Debug for ShowSchemasPlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for ShowSchemasPlanNode {
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
        // DROP MODEL
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ShowSchema")
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        _inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(ShowSchemasPlanNode {
            schema: Arc::new(DFSchema::empty()),
            like: self.like.clone(),
        })
    }
}

#[pyclass(name = "ShowSchema", module = "dask_planner", subclass)]
pub struct PyShowSchema {
    pub(crate) show_schema: ShowSchemasPlanNode,
}

#[pymethods]
impl PyShowSchema {
    #[pyo3(name = "getLike")]
    fn get_like(&self) -> PyResult<String> {
        match &self.show_schema.like {
            Some(e) => Ok(e.clone()),
            None => Ok("".to_string()),
        }
    }
}

impl TryFrom<logical::LogicalPlan> for PyShowSchema {
    type Error = PyErr;

    fn try_from(logical_plan: logical::LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            logical::LogicalPlan::Extension(extension) => {
                if let Some(ext) = extension
                    .node
                    .as_any()
                    .downcast_ref::<ShowSchemasPlanNode>()
                {
                    Ok(PyShowSchema {
                        show_schema: ext.clone(),
                    })
                } else {
                    Err(py_type_err("unexpected plan"))
                }
            }
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
