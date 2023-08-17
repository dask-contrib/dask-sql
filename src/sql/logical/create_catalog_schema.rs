use std::{
    any::Any,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

use datafusion_python::{
    datafusion_common::{DFSchema, DFSchemaRef},
    datafusion_expr::{logical_plan::UserDefinedLogicalNode, Expr, LogicalPlan},
};
use fmt::Debug;
use pyo3::prelude::*;

use crate::sql::{exceptions::py_type_err, logical};

#[derive(Clone, PartialEq)]
pub struct CreateCatalogSchemaPlanNode {
    pub schema: DFSchemaRef,
    pub schema_name: String,
    pub if_not_exists: bool,
    pub or_replace: bool,
}

impl Debug for CreateCatalogSchemaPlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl Hash for CreateCatalogSchemaPlanNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        self.schema_name.hash(state);
        self.if_not_exists.hash(state);
        self.or_replace.hash(state);
    }
}

impl UserDefinedLogicalNode for CreateCatalogSchemaPlanNode {
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
        // CREATE SCHEMA
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CreateCatalogSchema: schema_name={}, or_replace={}, if_not_exists={}",
            self.schema_name, self.or_replace, self.if_not_exists
        )
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        _inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(CreateCatalogSchemaPlanNode {
            schema: Arc::new(DFSchema::empty()),
            schema_name: self.schema_name.clone(),
            if_not_exists: self.if_not_exists,
            or_replace: self.or_replace,
        })
    }

    fn name(&self) -> &str {
        "CreateCatalogSchema"
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

#[pyclass(name = "CreateCatalogSchema", module = "dask_sql", subclass)]
pub struct PyCreateCatalogSchema {
    pub(crate) create_catalog_schema: CreateCatalogSchemaPlanNode,
}

#[pymethods]
impl PyCreateCatalogSchema {
    #[pyo3(name = "getSchemaName")]
    fn get_schema_name(&self) -> PyResult<String> {
        Ok(self.create_catalog_schema.schema_name.clone())
    }

    #[pyo3(name = "getIfNotExists")]
    fn get_if_not_exists(&self) -> PyResult<bool> {
        Ok(self.create_catalog_schema.if_not_exists)
    }

    #[pyo3(name = "getReplace")]
    fn get_replace(&self) -> PyResult<bool> {
        Ok(self.create_catalog_schema.or_replace)
    }
}

impl TryFrom<logical::LogicalPlan> for PyCreateCatalogSchema {
    type Error = PyErr;

    fn try_from(logical_plan: logical::LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            logical::LogicalPlan::Extension(extension) => {
                if let Some(ext) = extension
                    .node
                    .as_any()
                    .downcast_ref::<CreateCatalogSchemaPlanNode>()
                {
                    Ok(PyCreateCatalogSchema {
                        create_catalog_schema: ext.clone(),
                    })
                } else {
                    Err(py_type_err("unexpected plan"))
                }
            }
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
