use crate::sql::logical;
use crate::sql::types::rel_data_type::RelDataType;
use crate::sql::types::rel_data_type_field::RelDataTypeField;
use crate::sql::types::SqlTypeName;

use async_trait::async_trait;

use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub(crate) statistics: DaskStatistics,
    pub(crate) columns: Vec<(String, SqlTypeName)>,
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

    // TODO: Really wish we could accept a SqlTypeName instance here instead of a String for `column_type` ....
    #[pyo3(name = "add_column")]
    pub fn add_column(&mut self, column_name: String, column_type: String) {
        self.columns
            .push((column_name, SqlTypeName::from_string(&column_type)));
    }

    #[pyo3(name = "getQualifiedName")]
    pub fn qualified_name(&self, plan: logical::PyLogicalPlan) -> Vec<String> {
        let mut qualified_name = Vec::from([String::from("root")]);

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

    #[pyo3(name = "getRowType")]
    pub fn row_type(&self) -> RelDataType {
        let mut fields: Vec<RelDataTypeField> = Vec::new();
        for (name, data_type) in &self.columns {
            fields.push(RelDataTypeField::new(name.clone(), data_type.clone(), 255));
        }
        RelDataType::new(false, fields)
    }
}

/// Traverses the logical plan to locate the Table associated with the query
pub(crate) fn table_from_logical_plan(plan: &LogicalPlan) -> Option<DaskTable> {
    match plan {
        LogicalPlan::Projection(projection) => table_from_logical_plan(&projection.input),
        LogicalPlan::Filter(filter) => table_from_logical_plan(&filter.input),
        LogicalPlan::TableScan(table_scan) => {
            // Get the TableProvider for this Table instance
            let tbl_provider: Arc<dyn TableProvider> = table_scan.source.clone();
            let tbl_schema: SchemaRef = tbl_provider.schema();
            let fields: &Vec<Field> = tbl_schema.fields();

            let mut cols: Vec<(String, SqlTypeName)> = Vec::new();
            for field in fields {
                let data_type: &DataType = field.data_type();
                cols.push((
                    String::from(field.name()),
                    SqlTypeName::from_arrow(data_type),
                ));
            }

            Some(DaskTable {
                name: String::from(&table_scan.table_name),
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
