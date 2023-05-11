use std::sync::Arc;

use datafusion_python::{
    datafusion_expr::LogicalPlan,
    errors::py_unsupported_variant_err,
    sql::logical::PyLogicalPlan,
};
use pyo3::{pyclass, pymethods, IntoPy, PyObject, PyResult, Python};

use self::{
    aggregate::PyAggregate as DaskAggregate,
    alter_schema::{AlterSchemaPlanNode, PyAlterSchema},
    alter_table::{AlterTablePlanNode, PyAlterTable},
    analyze_table::{AnalyzeTablePlanNode, PyAnalyzeTable},
    create_catalog_schema::{CreateCatalogSchemaPlanNode, PyCreateCatalogSchema},
    create_experiment::{CreateExperimentPlanNode, PyCreateExperiment},
    create_model::{CreateModelPlanNode, PyCreateModel},
    create_table::{CreateTablePlanNode, PyCreateTable},
    describe_model::{DescribeModelPlanNode, PyDescribeModel},
    drop_model::{DropModelPlanNode, PyDropModel},
    drop_schema::{DropSchemaPlanNode, PyDropSchema},
    export_model::{ExportModelPlanNode, PyExportModel},
    predict_model::{PredictModelPlanNode, PyPredictModel},
    show_columns::{PyShowColumns, ShowColumnsPlanNode},
    show_models::{PyShowModels, ShowModelsPlanNode},
    show_schemas::{PyShowSchema, ShowSchemasPlanNode},
    show_tables::{PyShowTables, ShowTablesPlanNode},
    sort::PySort,
    use_schema::{PyUseSchema, UseSchemaPlanNode},
};

pub mod aggregate;
pub mod alter_schema;
pub mod alter_table;
pub mod analyze_table;
pub mod create_catalog_schema;
pub mod create_experiment;
pub mod create_memory_table;
pub mod create_model;
pub mod create_table;
pub mod describe_model;
pub mod drop_model;
pub mod drop_schema;
pub mod drop_table;
pub mod empty_relation;
pub mod explain;
pub mod export_model;
pub mod filter;
pub mod join;
pub mod limit;
pub mod predict_model;
pub mod projection;
pub mod repartition_by;
pub mod show_columns;
pub mod show_models;
pub mod show_schemas;
pub mod show_tables;
pub mod sort;
pub mod subquery_alias;
pub mod table_scan;
pub mod use_schema;
pub mod utils;
pub mod window;

#[derive(Debug, Clone)]
#[pyclass(name = "DaskLogicalPlan", module = "dask_planner", subclass)]
pub struct DaskLogicalPlan {
    pub plan: Arc<LogicalPlan>,
}

impl DaskLogicalPlan {
    pub fn new(plan: LogicalPlan) -> Self {
        DaskLogicalPlan {
            plan: Arc::new(plan),
        }
    }

    pub fn plan(&self) -> Arc<LogicalPlan> {
        self.plan.clone()
    }
}

#[pymethods]
impl DaskLogicalPlan {
    /// Return the specific logical operator
    fn to_variant(&self, py: Python) -> PyResult<PyObject> {
        Python::with_gil(|_| match self.plan.as_ref() {
            // We first check for custom LogicalNodes. These are nodes that are not part of ANSI SQL
            // and therefore cannot handled by Arrow DataFusion Python since they are unique to
            // dask-sql. Here we check for the existence of those nodes and parse them locally if
            // they exists. If the node is not a custom node then the processing is delegated
            // to Arrow DataFusion Python.
            LogicalPlan::Extension(extension) => {
                let node = extension.node.as_any();
                if node.downcast_ref::<CreateModelPlanNode>().is_some() {
                    Ok(PyCreateModel::try_from((*self.plan).clone())
                        .unwrap()
                        .into_py(py))
                } else if node.downcast_ref::<CreateExperimentPlanNode>().is_some() {
                    Ok(PyCreateExperiment::try_from((*self.plan).clone())
                        .unwrap()
                        .into_py(py))
                } else if node.downcast_ref::<CreateCatalogSchemaPlanNode>().is_some() {
                    Ok(PyCreateCatalogSchema::try_from((*self.plan).clone())
                        .unwrap()
                        .into_py(py))
                } else if node.downcast_ref::<CreateTablePlanNode>().is_some() {
                    Ok(PyCreateTable::try_from((*self.plan).clone())
                        .unwrap()
                        .into_py(py))
                } else if node.downcast_ref::<DropModelPlanNode>().is_some() {
                    Ok(PyDropModel::try_from((*self.plan).clone())
                        .unwrap()
                        .into_py(py))
                } else if node.downcast_ref::<PredictModelPlanNode>().is_some() {
                    Ok(PyPredictModel::try_from((*self.plan).clone())
                        .unwrap()
                        .into_py(py))
                } else if node.downcast_ref::<ExportModelPlanNode>().is_some() {
                    Ok(PyExportModel::try_from((*self.plan).clone())
                        .unwrap()
                        .into_py(py))
                } else if node.downcast_ref::<DescribeModelPlanNode>().is_some() {
                    Ok(PyDescribeModel::try_from((*self.plan).clone())
                        .unwrap()
                        .into_py(py))
                } else if node.downcast_ref::<ShowSchemasPlanNode>().is_some() {
                    Ok(PyShowSchema::try_from((*self.plan).clone())
                        .unwrap()
                        .into_py(py))
                } else if node.downcast_ref::<ShowTablesPlanNode>().is_some() {
                    Ok(PyShowTables::try_from((*self.plan).clone())
                        .unwrap()
                        .into_py(py))
                } else if node.downcast_ref::<ShowColumnsPlanNode>().is_some() {
                    Ok(PyShowColumns::try_from((*self.plan).clone())
                        .unwrap()
                        .into_py(py))
                } else if node.downcast_ref::<ShowModelsPlanNode>().is_some() {
                    Ok(PyShowModels::try_from((*self.plan).clone())
                        .unwrap()
                        .into_py(py))
                } else if node.downcast_ref::<DropSchemaPlanNode>().is_some() {
                    Ok(PyDropSchema::try_from((*self.plan).clone())
                        .unwrap()
                        .into_py(py))
                } else if node.downcast_ref::<UseSchemaPlanNode>().is_some() {
                    Ok(PyUseSchema::try_from((*self.plan).clone())
                        .unwrap()
                        .into_py(py))
                } else if node.downcast_ref::<AnalyzeTablePlanNode>().is_some() {
                    Ok(PyAnalyzeTable::try_from((*self.plan).clone())
                        .unwrap()
                        .into_py(py))
                } else if node.downcast_ref::<AlterTablePlanNode>().is_some() {
                    Ok(PyAlterTable::try_from((*self.plan).clone())
                        .unwrap()
                        .into_py(py))
                } else if node.downcast_ref::<AlterSchemaPlanNode>().is_some() {
                    Ok(PyAlterSchema::try_from((*self.plan).clone())
                        .unwrap()
                        .into_py(py))
                } else {
                    Err(py_unsupported_variant_err(format!(
                        "Cannot convert this plan to a LogicalNode: {:?}",
                        *self.plan
                    )))
                }
            }

            // We handle Aggregate and Distinct a little differently than ADP. Enough of a difference
            // that we choose to custom handle those here.
            LogicalPlan::Aggregate(_) | LogicalPlan::Distinct(_) => {
                Ok(DaskAggregate::try_from((*self.plan).clone())?.into_py(py))
            }

            // Sort logic should remain here for the time being
            LogicalPlan::Sort(_) => Ok(PySort::try_from((*self.plan).clone())?.into_py(py)),

            // Delegate processing to Arrow DataFusion Python
            other => PyLogicalPlan::new((*other).clone()).to_variant(py),
        })
    }

    /// Get the inputs to this plan
    fn inputs(&self) -> Vec<DaskLogicalPlan> {
        let mut inputs = vec![];
        for input in self.plan.inputs() {
            inputs.push(input.to_owned().into());
        }
        inputs
    }

    /// Consumes the current DaskLogicalPlan instance
    /// into a native datafusion `LogicalPlan`
    fn datafusion_plan(&self) -> PyLogicalPlan {
        Into::<PyLogicalPlan>::into((*self.plan).clone())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.plan))
    }

    fn display(&self) -> String {
        format!("{}", self.plan.display())
    }

    fn display_indent(&self) -> String {
        format!("{}", self.plan.display_indent())
    }

    fn display_indent_schema(&self) -> String {
        format!("{}", self.plan.display_indent_schema())
    }

    fn display_graphviz(&self) -> String {
        format!("{}", self.plan.display_graphviz())
    }
}

impl From<DaskLogicalPlan> for LogicalPlan {
    fn from(logical_plan: DaskLogicalPlan) -> LogicalPlan {
        logical_plan.plan.as_ref().clone()
    }
}

impl From<LogicalPlan> for DaskLogicalPlan {
    fn from(logical_plan: LogicalPlan) -> DaskLogicalPlan {
        DaskLogicalPlan {
            plan: Arc::new(logical_plan),
        }
    }
}
