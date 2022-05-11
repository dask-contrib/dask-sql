use crate::sql::logical;
use crate::sql::types::rel_data_type::RelDataType;
use crate::sql::types::rel_data_type_field::RelDataTypeField;
use crate::sql::types::DaskTypeMap;
use crate::sql::types::SqlTypeName;

use async_trait::async_trait;

use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
pub use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, LogicalPlan, TableSource};
use datafusion::physical_plan::{empty::EmptyExec, project_schema, ExecutionPlan};

use pyo3::prelude::*;

use std::any::Any;
use std::sync::Arc;

/// DaskTable wrapper that is compatible with DataFusion logical query plans
pub struct DaskTableSource {
    schema: SchemaRef,
}

impl DaskTableSource {
    /// Initialize a new `EmptyTable` from a schema.
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

/// Implement TableSource, used in the logical query plan and in logical query optimizations
#[async_trait]
impl TableSource for DaskTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// DaskTable wrapper that is compatible with DataFusion physical query plans
pub struct DaskTableProvider {
    source: Arc<DaskTableSource>,
}

impl DaskTableProvider {
    pub fn new(source: Arc<DaskTableSource>) -> Self {
        Self { source }
    }
}

/// Implement TableProvider, used for physical query plans and execution
#[async_trait]
impl TableProvider for DaskTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.source.schema.clone()
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // even though there is no data, projections apply
        let projected_schema = project_schema(&self.source.schema, projection.as_ref())?;
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
    pub(crate) columns: Vec<(String, DaskTypeMap)>,
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
    pub fn add_column(&mut self, column_name: String, type_map: DaskTypeMap) {
        self.columns.push((column_name, type_map));
    }

    #[pyo3(name = "getQualifiedName")]
    pub fn qualified_name(&self, plan: logical::PyLogicalPlan) -> Vec<String> {
        let mut qualified_name = Vec::from([String::from("root")]);

        match plan.original_plan {
            LogicalPlan::TableScan(table_scan) => {
                qualified_name.push(table_scan.table_name);
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
            let tbl_provider: Arc<dyn TableSource> = table_scan.source.clone();
            let tbl_schema: SchemaRef = tbl_provider.schema();
            let fields: &Vec<Field> = tbl_schema.fields();

            let mut cols: Vec<(String, DaskTypeMap)> = Vec::new();
            for field in fields {
                let data_type: &DataType = field.data_type();
                cols.push((
                    String::from(field.name()),
                    DaskTypeMap::from(SqlTypeName::from_arrow(data_type), data_type.clone()),
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
