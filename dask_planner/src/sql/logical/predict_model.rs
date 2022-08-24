use crate::sql::exceptions::py_type_err;
use crate::sql::logical;
use pyo3::prelude::*;

use datafusion_expr::logical_plan::UserDefinedLogicalNode;
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_sql::sqlparser::ast::{Expr as SqlParserExpr, Value};

use fmt::Debug;
use std::collections::HashMap;
use std::{any::Any, fmt, sync::Arc};

use datafusion_common::DFSchemaRef;

#[derive(Clone)]
pub struct PredictModelPlanNode {
    pub model_name: String,
    pub input: LogicalPlan,
    pub with_options: Vec<SqlParserExpr>,
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
        // CREATE TABLE
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
            model_name: self.model_name.clone(),
            input: inputs[0].clone(),
            with_options: self.with_options.clone(),
        })
    }
}

#[pyclass(name = "PredictModel", module = "dask_planner", subclass)]
pub struct PyPredictModel {
    pub(crate) predict_model: PredictModelPlanNode,
}

#[pymethods]
impl PyPredictModel {
    #[pyo3(name = "getModelName")]
    fn get_model_name(&self) -> PyResult<String> {
        Ok(self.predict_model.model_name.clone())
    }

    #[pyo3(name = "getSQLWithOptions")]
    fn sql_with_options(&self) -> PyResult<HashMap<String, String>> {
        let mut options: HashMap<String, String> = HashMap::new();
        for elem in &self.predict_model.with_options {
            if let SqlParserExpr::BinaryOp { left, op: _, right } = elem {
                let key: Result<String, PyErr> = match *left.clone() {
                    SqlParserExpr::Identifier(ident) => Ok(ident.value),
                    _ => Err(py_type_err(format!(
                        "unexpected `left` Value type encountered: {:?}",
                        left
                    ))),
                };
                let val: Result<String, PyErr> = match *right.clone() {
                    SqlParserExpr::Value(value) => match value {
                        Value::SingleQuotedString(e) => Ok(e.replace('\'', "")),
                        Value::DoubleQuotedString(e) => Ok(e.replace('\"', "")),
                        Value::Boolean(e) => {
                            if e {
                                Ok("True".to_string())
                            } else {
                                Ok("False".to_string())
                            }
                        }
                        Value::Number(e, ..) => Ok(e),
                        _ => Err(py_type_err(format!(
                            "unexpected Value type encountered: {:?}",
                            value
                        ))),
                    },
                    _ => Err(py_type_err(format!(
                        "encountered unexpected Expr type: {:?}",
                        right
                    ))),
                };
                options.insert(key?, val?);
            }
        }
        Ok(options)
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
