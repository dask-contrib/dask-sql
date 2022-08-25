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
pub struct CreateModelPlanNode {
    pub model_name: String,
    pub input: LogicalPlan,
    pub if_not_exists: bool,
    pub or_replace: bool,
    pub with_options: Vec<SqlParserExpr>,
}

impl Debug for CreateModelPlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for CreateModelPlanNode {
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
        // CREATE MODEL
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CreateModel: model_name={}", self.model_name)
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        Arc::new(CreateModelPlanNode {
            model_name: self.model_name.clone(),
            input: inputs[0].clone(),
            if_not_exists: self.if_not_exists,
            or_replace: self.or_replace,
            with_options: self.with_options.clone(),
        })
    }
}

#[pyclass(name = "CreateModel", module = "dask_planner", subclass)]
pub struct PyCreateModel {
    pub(crate) create_model: CreateModelPlanNode,
}

#[pymethods]
impl PyCreateModel {
    /// Creating a model requires that a subquery be passed to the CREATE MODEL
    /// statement to be used to gather the dataset which should be used for the
    /// model. This function returns that portion of the statement.
    #[pyo3(name = "getSelectQuery")]
    fn get_select_query(&self) -> PyResult<logical::PyLogicalPlan> {
        Ok(self.create_model.input.clone().into())
    }

    #[pyo3(name = "getModelName")]
    fn get_model_name(&self) -> PyResult<String> {
        Ok(self.create_model.model_name.clone())
    }

    #[pyo3(name = "getIfNotExists")]
    fn get_if_not_exists(&self) -> PyResult<bool> {
        Ok(self.create_model.if_not_exists)
    }

    #[pyo3(name = "getOrReplace")]
    pub fn get_or_replace(&self) -> PyResult<bool> {
        Ok(self.create_model.or_replace)
    }

    #[pyo3(name = "getSQLWithOptions")]
    fn sql_with_options(&self) -> PyResult<HashMap<String, String>> {
        let mut options: HashMap<String, String> = HashMap::new();
        for elem in &self.create_model.with_options {
            match elem {
                SqlParserExpr::BinaryOp { left, op: _, right } => {
                    options.insert(
                        Self::_str_from_expr(*left.clone()),
                        Self::_str_from_expr(*right.clone()),
                    );
                }
                _ => {
                    return Err(py_type_err(
                        "Encountered non SqlParserExpr::BinaryOp expression, with arguments can only be of Key/Value pair types"));
                }
            }
        }
        Ok(options)
    }
}

impl PyCreateModel {
    /// Given a SqlParserExpr instance retrieve the String value from it
    fn _str_from_expr(expression: SqlParserExpr) -> String {
        match expression {
            SqlParserExpr::Identifier(ident) => ident.value,
            SqlParserExpr::Value(value) => match value {
                Value::SingleQuotedString(e) => e.replace('\'', ""),
                Value::DoubleQuotedString(e) => e.replace('\"', ""),
                Value::Boolean(e) => {
                    if e {
                        "True".to_string()
                    } else {
                        "False".to_string()
                    }
                }
                Value::Number(e, ..) => e,
                _ => unimplemented!("Unimplmented Value type: {:?}", value),
            },
            SqlParserExpr::Nested(nested_expr) => Self::_str_from_expr(*nested_expr),
            _ => unimplemented!("Unimplmented SqlParserExpr type: {:?}", expression),
        }
    }
}

impl TryFrom<logical::LogicalPlan> for PyCreateModel {
    type Error = PyErr;

    fn try_from(logical_plan: logical::LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            logical::LogicalPlan::Extension(extension) => {
                if let Some(ext) = extension
                    .node
                    .as_any()
                    .downcast_ref::<CreateModelPlanNode>()
                {
                    Ok(PyCreateModel {
                        create_model: ext.clone(),
                    })
                } else {
                    Err(py_type_err("unexpected plan"))
                }
            }
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
