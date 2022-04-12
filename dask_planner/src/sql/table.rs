use crate::sql::logical;
use crate::sql::types;

use async_trait::async_trait;

use datafusion::arrow::datatypes::SchemaRef;
pub use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_plan::plan::LogicalPlan;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::{empty::EmptyExec, project_schema, ExecutionPlan};

use pyo3::prelude::*;

use std::any::Any;
use std::sync::Arc;

/// DaskTableProvider
pub struct DaskTableProvider {
    schema: SchemaRef,
    table_name: String,
}

impl DaskTableProvider {
    /// Initialize a new `EmptyTable` from a schema.
    pub fn new(schema: SchemaRef, table_name: String) -> Self {
        Self { schema, table_name }
    }

    pub fn table_name(&self) -> String {
        self.table_name.clone()
    }
}

#[async_trait]
impl TableProvider for DaskTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // even though there is no data, projections apply
        let projected_schema = project_schema(&self.schema, projection.as_ref())?;
        Ok(Arc::new(EmptyExec::new(false, projected_schema)))
    }
}

#[pyclass(name = "DaskStatistics", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct DaskStatistics {
    row_count: f64,
}

#[pymethods]
impl DaskStatistics {
    #[new]
    pub fn new(row_count: f64) -> Self {
        Self { row_count }
    }
}

#[pyclass(name = "DaskTable", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct DaskTable {
    pub(crate) name: String,
    pub(crate) statistics: DaskStatistics,
    pub(crate) columns: Vec<(String, types::DaskRelDataType)>,
}

#[pymethods]
impl DaskTable {
    #[new]
    pub fn new(table_name: String, row_count: f64) -> Self {
        Self {
            name: table_name,
            statistics: DaskStatistics::new(row_count),
            columns: Vec::new(),
        }
    }

    pub fn add_column(&mut self, column_name: String, column_type_str: String) {
        let sql_type: types::DaskRelDataType = types::DaskRelDataType {
            name: String::from(&column_name),
            sql_type: types::sql_type_to_arrow_type(column_type_str),
        };

        self.columns.push((column_name, sql_type));
    }

    pub fn get_qualified_name(&self, plan: logical::PyLogicalPlan) -> Vec<String> {
        let mut qualified_name = Vec::new();

        //TODO: What scope should the current schema be pulled from here?? For now temporary hardcoded to default "root"
        qualified_name.push(String::from("root"));

        match plan.original_plan {
            LogicalPlan::TableScan(_table_scan) => {
                let tbl = _table_scan
                    .source
                    .as_any()
                    .downcast_ref::<DaskTableProvider>()
                    .unwrap()
                    .table_name();
                qualified_name.push(tbl);
            }
            _ => {
                println!("Nothing matches");
                qualified_name.push(self.name.clone());
            }
        }

        qualified_name
    }

    pub fn column_names(&self) -> Vec<String> {
        let mut cns: Vec<String> = Vec::new();
        for c in &self.columns {
            cns.push(String::from(&c.0));
        }
        cns
    }

    pub fn column_types(&self) -> Vec<types::DaskRelDataType> {
        let mut col_types: Vec<types::DaskRelDataType> = Vec::new();
        for col in &self.columns {
            col_types.push(col.1.clone())
        }
        col_types
    }

    pub fn num_columns(&self) {
        println!(
            "There are {} columns in table {}",
            self.columns.len(),
            self.name
        );
    }
}

/// Traverses the logical plan to locate the Table associated with the query
pub(crate) fn table_from_logical_plan(plan: &LogicalPlan) -> Option<DaskTable> {
    match plan {
        LogicalPlan::Projection(projection) => table_from_logical_plan(&projection.input),
        LogicalPlan::Filter(filter) => table_from_logical_plan(&filter.input),
        LogicalPlan::TableScan(tableScan) => {
            // Get the TableProvider for this Table instance
            let tbl_provider: Arc<dyn TableProvider> = tableScan.source.clone();
            let tbl_schema: SchemaRef = tbl_provider.schema();
            let fields = tbl_schema.fields();

            let mut cols: Vec<(String, types::DaskRelDataType)> = Vec::new();
            for field in fields {
                cols.push((
                    String::from(field.name()),
                    types::DaskRelDataType {
                        name: String::from(field.name()),
                        sql_type: field.data_type().clone(),
                    },
                ));
            }

            Some(DaskTable {
                name: String::from(&tableScan.table_name),
                statistics: DaskStatistics { row_count: 0.0 },
                columns: cols,
            })
        }
        LogicalPlan::Join(join) => {
            //TODO: Don't always hardcode the left
            table_from_logical_plan(&join.left)
        }
        LogicalPlan::Aggregate(agg) => table_from_logical_plan(&agg.input),
        _ => todo!("table_from_logical_plan: unimplemented LogicalPlan type encountered"),
    }
}
