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
pub struct ShowSchemasPlanNode {
    pub schema: DFSchemaRef,
    pub catalog_name: Option<String>,
    pub like: Option<String>,
}

impl Debug for ShowSchemasPlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl Hash for ShowSchemasPlanNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        self.like.hash(state);
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
        // SHOW SCHEMAS
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ShowSchema: catalog_name: {:?}", self.catalog_name)
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        _inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(ShowSchemasPlanNode {
            schema: Arc::new(DFSchema::empty()),
            catalog_name: self.catalog_name.clone(),
            like: self.like.clone(),
        })
    }

    fn name(&self) -> &str {
        "ShowSchema"
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

#[pyclass(name = "ShowSchema", module = "dask_sql", subclass)]
pub struct PyShowSchema {
    pub(crate) show_schema: ShowSchemasPlanNode,
}

#[pymethods]
impl PyShowSchema {
    #[pyo3(name = "getCatalogName")]
    fn get_from(&self) -> PyResult<Option<String>> {
        Ok(self.show_schema.catalog_name.clone())
    }

    #[pyo3(name = "getLike")]
    fn get_like(&self) -> PyResult<Option<String>> {
        Ok(self.show_schema.like.clone())
    }
}

impl TryFrom<logical::LogicalPlan> for PyShowSchema {
    type Error = PyErr;

    fn try_from(logical_plan: logical::LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Extension(Extension { node })
                if node
                    .as_any()
                    .downcast_ref::<ShowSchemasPlanNode>()
                    .is_some() =>
            {
                let ext = node
                    .as_any()
                    .downcast_ref::<ShowSchemasPlanNode>()
                    .expect("ShowSchemasPlanNode");
                Ok(PyShowSchema {
                    show_schema: ext.clone(),
                })
            }
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
