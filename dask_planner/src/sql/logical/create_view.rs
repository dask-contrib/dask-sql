use std::{any::Any, fmt, sync::Arc};

use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::{logical_plan::UserDefinedLogicalNode, Expr, LogicalPlan};
use fmt::Debug;
use pyo3::prelude::*;

use crate::sql::{exceptions::py_type_err, logical};

#[derive(Clone)]
pub struct CreateViewPlanNode {
    pub schema: DFSchemaRef,
    pub view_schema: String, // "something" in `something.table_name`
    pub view_name: String,
    pub if_not_exists: bool,
    pub or_replace: bool,
}

impl Debug for CreateViewPlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for CreateViewPlanNode {
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
        // CREATE VIEW
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CreateView: view_schema={}, view_name={}, if_not_exists={}, or_replace={}",
            self.view_schema, self.view_name, self.if_not_exists, self.or_replace
        )
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        _inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(CreateViewPlanNode {
            schema: Arc::new(DFSchema::empty()),
            view_schema: self.view_schema.clone(),
            view_name: self.view_name.clone(),
            if_not_exists: self.if_not_exists,
            or_replace: self.or_replace,
        })
    }
}

#[pyclass(name = "CreateView", module = "dask_planner", subclass)]
pub struct PyCreateView {
    pub(crate) create_view: CreateViewPlanNode,
}

#[pymethods]
impl PyCreateView {
    #[pyo3(name = "getViewSchema")]
    fn get_view_schema(&self) -> PyResult<String> {
        Ok(self.create_view.view_schema.clone())
    }

    #[pyo3(name = "getViewName")]
    fn get_view_name(&self) -> PyResult<String> {
        Ok(self.create_view.view_name.clone())
    }

    #[pyo3(name = "getIfNotExists")]
    fn get_if_not_exists(&self) -> PyResult<bool> {
        Ok(self.create_view.if_not_exists)
    }

    #[pyo3(name = "getOrReplace")]
    fn get_or_replace(&self) -> PyResult<bool> {
        Ok(self.create_view.or_replace)
    }
}

impl TryFrom<logical::LogicalPlan> for PyCreateView {
    type Error = PyErr;

    fn try_from(logical_plan: logical::LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            logical::LogicalPlan::Extension(extension) => {
                if let Some(ext) = extension.node.as_any().downcast_ref::<CreateViewPlanNode>() {
                    Ok(PyCreateView {
                        create_view: ext.clone(),
                    })
                } else {
                    Err(py_type_err("unexpected plan"))
                }
            }
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
