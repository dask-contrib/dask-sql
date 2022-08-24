use crate::sql::exceptions::py_type_err;
use crate::sql::logical;
use pyo3::prelude::*;

use datafusion_expr::logical_plan::UserDefinedLogicalNode;
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_sql::sqlparser::ast::{Expr as SqlParserExpr, Value};

use fmt::Debug;
use std::collections::HashMap;
use std::{any::Any, fmt, sync::Arc};

use datafusion_common::{DFSchema, DFSchemaRef};

#[derive(Clone)]
pub struct CreateTablePlanNode {
    pub schema: DFSchemaRef,
    pub table_schema: String, // "something" in `something.table_name`
    pub table_name: String,
    pub if_not_exists: bool,
    pub or_replace: bool,
    pub with_options: Vec<SqlParserExpr>,
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
            table_schema: self.table_schema.clone(),
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
    #[pyo3(name = "getTableName")]
    fn get_table_name(&self) -> PyResult<String> {
        Ok(self.create_table.table_name.clone())
    }

    #[pyo3(name = "getIfNotExists")]
    fn get_if_not_exists(&self) -> PyResult<bool> {
        Ok(self.create_table.if_not_exists)
    }

    #[pyo3(name = "getReplace")]
    fn get_replace(&self) -> PyResult<bool> {
        Ok(self.create_table.or_replace)
    }

    #[pyo3(name = "getSQLWithOptions")]
    fn sql_with_options(&self) -> PyResult<HashMap<String, String>> {
        let mut options: HashMap<String, String> = HashMap::new();
        for elem in &self.create_table.with_options {
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
