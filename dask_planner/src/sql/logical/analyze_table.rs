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
pub struct AnalyzeTablePlanNode {
    pub schema: DFSchemaRef,
    pub table_name: String,
    pub schema_name: Option<String>,
    pub columns: Vec<String>,
}

impl Debug for AnalyzeTablePlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for AnalyzeTablePlanNode {
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
        // ANALYZE TABLE {table_name}
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Analyze Table: table_name: {:?}, columns: {:?}",
            self.table_name, self.columns
        )
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        _inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(AnalyzeTablePlanNode {
            schema: Arc::new(DFSchema::empty()),
            table_name: self.table_name.clone(),
            schema_name: self.schema_name.clone(),
            columns: self.columns.clone(),
        })
    }
}

#[pyclass(name = "AnalyzeTable", module = "dask_planner", subclass)]
pub struct PyAnalyzeTable {
    pub(crate) analyze_table: AnalyzeTablePlanNode,
}

#[pymethods]
impl PyAnalyzeTable {
    #[pyo3(name = "getTableName")]
    fn get_table_name(&self) -> PyResult<String> {
        Ok(self.analyze_table.table_name.clone())
    }

    #[pyo3(name = "getSchemaName")]
    fn get_schema_name(&self) -> PyResult<Option<String>> {
        Ok(self.analyze_table.schema_name.clone())
    }

    #[pyo3(name = "getColumns")]
    fn get_columns(&self) -> PyResult<Vec<String>> {
        Ok(self.analyze_table.columns.clone())
    }
}

impl TryFrom<logical::LogicalPlan> for PyAnalyzeTable {
    type Error = PyErr;

    fn try_from(logical_plan: logical::LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Extension(Extension { node })
                if node
                    .as_any()
                    .downcast_ref::<AnalyzeTablePlanNode>()
                    .is_some() =>
            {
                let ext = node
                    .as_any()
                    .downcast_ref::<AnalyzeTablePlanNode>()
                    .expect("AnalyzeTablePlanNode");
                Ok(PyAnalyzeTable {
                    analyze_table: ext.clone(),
                })
            }
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
