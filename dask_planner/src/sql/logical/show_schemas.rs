use std::{any::Any, fmt, sync::Arc};

use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::{
    logical_plan::{Extension, UserDefinedLogicalNode},
    Expr,
    LogicalPlan,
};
use fmt::Debug;
use pyo3::prelude::*;

use crate::sql::{exceptions::py_type_err, logical};

#[derive(Clone)]
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
}

#[pyclass(name = "ShowSchema", module = "dask_planner", subclass)]
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
