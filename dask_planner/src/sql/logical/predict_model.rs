use std::{any::Any, fmt, sync::Arc};

use datafusion_common::DFSchemaRef;
use datafusion_expr::{logical_plan::UserDefinedLogicalNode, Expr, LogicalPlan};
use fmt::Debug;
use pyo3::prelude::*;

use super::PyLogicalPlan;
use crate::sql::{exceptions::py_type_err, logical};

#[derive(Clone)]
pub struct PredictModelPlanNode {
    pub schema_name: Option<String>, // "something" in `something.model_name`
    pub model_name: String,
    pub input: LogicalPlan,
}

impl Debug for PredictModelPlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for PredictModelPlanNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        // there is no need to expose any expressions here since DataFusion would
        // not be able to do anything with expressions that are specific to
        // PREDICT TABLE
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PredictModel: model_name={}", self.model_name)
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(PredictModelPlanNode {
            schema_name: self.schema_name.clone(),
            model_name: self.model_name.clone(),
            input: inputs[0].clone(),
        })
    }
}

#[pyclass(name = "PredictModel", module = "dask_planner", subclass)]
pub struct PyPredictModel {
    pub(crate) predict_model: PredictModelPlanNode,
}

#[pymethods]
impl PyPredictModel {
    #[pyo3(name = "getSchemaName")]
    fn get_schema_name(&self) -> PyResult<Option<String>> {
        Ok(self.predict_model.schema_name.clone())
    }

    #[pyo3(name = "getModelName")]
    fn get_model_name(&self) -> PyResult<String> {
        Ok(self.predict_model.model_name.clone())
    }

    #[pyo3(name = "getSelect")]
    fn get_select(&self) -> PyResult<PyLogicalPlan> {
        Ok(PyLogicalPlan::from(self.predict_model.input.clone()))
    }
}

impl TryFrom<logical::LogicalPlan> for PyPredictModel {
    type Error = PyErr;

    fn try_from(logical_plan: logical::LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            logical::LogicalPlan::Extension(extension) => {
                if let Some(ext) = extension
                    .node
                    .as_any()
                    .downcast_ref::<PredictModelPlanNode>()
                {
                    Ok(PyPredictModel {
                        predict_model: ext.clone(),
                    })
                } else {
                    Err(py_type_err("unexpected plan"))
                }
            }
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
