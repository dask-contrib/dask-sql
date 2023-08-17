use std::{
    any::Any,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

use datafusion_python::{
    datafusion_common::{DFSchema, DFSchemaRef},
    datafusion_expr::{
        logical_plan::{Extension, UserDefinedLogicalNode},
        Expr,
        LogicalPlan,
    },
};
use fmt::Debug;
use pyo3::prelude::*;

use crate::sql::{exceptions::py_type_err, logical};

#[derive(Clone, PartialEq)]
pub struct ShowColumnsPlanNode {
    pub schema: DFSchemaRef,
    pub table_name: String,
    pub schema_name: Option<String>,
}

impl Debug for ShowColumnsPlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl Hash for ShowColumnsPlanNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        self.table_name.hash(state);
        self.schema_name.hash(state);
    }
}

impl UserDefinedLogicalNode for ShowColumnsPlanNode {
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
        // SHOW COLUMNS FROM {table_name}
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Show Columns: table_name: {:?}", self.table_name)
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        _inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(ShowColumnsPlanNode {
            schema: Arc::new(DFSchema::empty()),
            table_name: self.table_name.clone(),
            schema_name: self.schema_name.clone(),
        })
    }

    fn name(&self) -> &str {
        "ShowColumns"
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

#[pyclass(name = "ShowColumns", module = "dask_sql", subclass)]
pub struct PyShowColumns {
    pub(crate) show_columns: ShowColumnsPlanNode,
}

#[pymethods]
impl PyShowColumns {
    #[pyo3(name = "getTableName")]
    fn get_table_name(&self) -> PyResult<String> {
        Ok(self.show_columns.table_name.clone())
    }

    #[pyo3(name = "getSchemaName")]
    fn get_schema_name(&self) -> PyResult<Option<String>> {
        Ok(self.show_columns.schema_name.clone())
    }
}

impl TryFrom<logical::LogicalPlan> for PyShowColumns {
    type Error = PyErr;

    fn try_from(logical_plan: logical::LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Extension(Extension { node })
                if node
                    .as_any()
                    .downcast_ref::<ShowColumnsPlanNode>()
                    .is_some() =>
            {
                let ext = node
                    .as_any()
                    .downcast_ref::<ShowColumnsPlanNode>()
                    .expect("ShowColumnsPlanNode");
                Ok(PyShowColumns {
                    show_columns: ext.clone(),
                })
            }
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
