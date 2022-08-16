use crate::sql::exceptions::py_type_err;
use crate::sql::logical;
use pyo3::prelude::*;

use datafusion_expr::logical_plan::UserDefinedLogicalNode;
use datafusion_expr::{Expr, LogicalPlan};

use fmt::Debug;
use std::{any::Any, fmt, sync::Arc};

use datafusion_common::DFSchemaRef;

#[derive(Clone)]
pub struct DropModelPlanNode {
    pub model_name: String,
}

impl Debug for DropModelPlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for DropModelPlanNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        // there is no need to expose any expressions here since DataFusion would
        // not be able to do anything with expressions that are specific to
        // DROP MODEL
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DropModel: model_name={}", self.model_name)
    }

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        Arc::new(DropModelPlanNode {
            model_name: self.model_name.clone(),
        })
    }
}

#[pyclass(name = "DropModel", module = "dask_planner", subclass)]
pub struct PyDropModel {
    pub(crate) drop_model: DropModelPlanNode,
}

#[pymethods]
impl PyDropModel {
    #[pyo3(name = "getModelName")]
    fn get_model_name(&self) -> PyResult<String> {
        Ok(self.drop_model.model_name.clone())
    }
}

impl TryFrom<logical::LogicalPlan> for PyDropModel {
    type Error = PyErr;

    fn try_from(logical_plan: logical::LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            logical::LogicalPlan::Extension(extension) => {
                if let Some(ext) = extension.node.as_any().downcast_ref::<DropModelPlanNode>() {
                    Ok(PyDropModel {
                        drop_model: ext.clone(),
                    })
                } else {
                    Err(py_type_err("unexpected plan"))
                }
            }
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
