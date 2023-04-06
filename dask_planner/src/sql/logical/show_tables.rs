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
pub struct ShowTablesPlanNode {
    pub schema: DFSchemaRef,
    pub catalog_name: Option<String>,
    pub schema_name: Option<String>,
}

impl Debug for ShowTablesPlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for ShowTablesPlanNode {
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
        // SHOW TABLES FROM {schema_name}
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ShowTables: catalog_name: {:?}, schema_name: {:?}",
            self.catalog_name, self.schema_name
        )
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        _inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(ShowTablesPlanNode {
            schema: Arc::new(DFSchema::empty()),
            catalog_name: self.catalog_name.clone(),
            schema_name: self.schema_name.clone(),
        })
    }
}

#[pyclass(name = "ShowTables", module = "dask_planner", subclass)]
pub struct PyShowTables {
    pub(crate) show_tables: ShowTablesPlanNode,
}

#[pymethods]
impl PyShowTables {
    #[pyo3(name = "getCatalogName")]
    fn get_catalog_name(&self) -> PyResult<Option<String>> {
        Ok(self.show_tables.catalog_name.clone())
    }

    #[pyo3(name = "getSchemaName")]
    fn get_schema_name(&self) -> PyResult<Option<String>> {
        Ok(self.show_tables.schema_name.clone())
    }
}

impl TryFrom<logical::LogicalPlan> for PyShowTables {
    type Error = PyErr;

    fn try_from(logical_plan: logical::LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Extension(Extension { node })
                if node.as_any().downcast_ref::<ShowTablesPlanNode>().is_some() =>
            {
                let ext = node
                    .as_any()
                    .downcast_ref::<ShowTablesPlanNode>()
                    .expect("ShowTablesPlanNode");
                Ok(PyShowTables {
                    show_tables: ext.clone(),
                })
            }
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
