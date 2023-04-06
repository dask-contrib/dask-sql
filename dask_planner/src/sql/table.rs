use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion_common::DFField;
use datafusion_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableSource};
use datafusion_optimizer::utils::split_conjunction;
use datafusion_sql::TableReference;
use pyo3::prelude::*;

use super::logical::{create_table::CreateTablePlanNode, predict_model::PredictModelPlanNode};
use crate::{
    error::DaskPlannerError,
    sql::{
        logical,
        types::{
            rel_data_type::RelDataType,
            rel_data_type_field::RelDataTypeField,
            DaskTypeMap,
            SqlTypeName,
        },
    },
};

/// DaskTable wrapper that is compatible with DataFusion logical query plans
pub struct DaskTableSource {
    schema: SchemaRef,
    statistics: Option<DaskStatistics>,
    filepath: Option<String>,
}

impl DaskTableSource {
    /// Initialize a new `EmptyTable` from a schema
    pub fn new(
        schema: SchemaRef,
        statistics: Option<DaskStatistics>,
        filepath: Option<String>,
    ) -> Self {
        Self {
            schema,
            statistics,
            filepath,
        }
    }

    /// Access optional statistics associated with this table source
    pub fn statistics(&self) -> Option<&DaskStatistics> {
        self.statistics.as_ref()
    }

    /// Access optional filepath associated with this table source
    #[allow(dead_code)]
    pub fn filepath(&self) -> Option<&String> {
        self.filepath.as_ref()
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

    fn supports_filter_pushdown(
        &self,
        filter: &Expr,
    ) -> datafusion_common::Result<TableProviderFilterPushDown> {
        let filters = split_conjunction(filter);
        if filters.iter().all(|f| is_supported_push_down_expr(f)) {
            // Push down filters to the tablescan operation if all are supported
            Ok(TableProviderFilterPushDown::Exact)
        } else if filters.iter().any(|f| is_supported_push_down_expr(f)) {
            // Partially apply the filter in the TableScan but retain
            // the Filter operator in the plan as well
            Ok(TableProviderFilterPushDown::Inexact)
        } else {
            Ok(TableProviderFilterPushDown::Unsupported)
        }
    }
}

fn is_supported_push_down_expr(_expr: &Expr) -> bool {
    // For now we support all kinds of expr's at this level
    true
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

    #[pyo3(name = "getRowCount")]
    pub fn get_row_count(&self) -> f64 {
        self.row_count
    }
}

#[pyclass(name = "DaskTable", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct DaskTable {
    pub(crate) schema_name: Option<String>,
    pub(crate) table_name: String,
    pub(crate) statistics: DaskStatistics,
    pub(crate) columns: Vec<(String, DaskTypeMap)>,
    pub(crate) filepath: Option<String>,
}

#[pymethods]
impl DaskTable {
    #[new]
    pub fn new(
        schema_name: &str,
        table_name: &str,
        row_count: f64,
        columns: Option<Vec<(String, DaskTypeMap)>>,
        filepath: Option<String>,
    ) -> Self {
        Self {
            schema_name: Some(schema_name.to_owned()),
            table_name: table_name.to_owned(),
            statistics: DaskStatistics::new(row_count),
            columns: columns.unwrap_or_default(),
            filepath,
        }
    }

    // TODO: Really wish we could accept a SqlTypeName instance here instead of a String for `column_type` ....
    #[pyo3(name = "add_column")]
    pub fn add_column(&mut self, column_name: &str, type_map: DaskTypeMap) {
        self.columns.push((column_name.to_owned(), type_map));
    }

    #[pyo3(name = "getSchema")]
    pub fn get_schema(&self) -> PyResult<Option<String>> {
        Ok(self.schema_name.clone())
    }

    #[pyo3(name = "getTableName")]
    pub fn get_table_name(&self) -> PyResult<String> {
        Ok(self.table_name.clone())
    }

    #[pyo3(name = "getQualifiedName")]
    pub fn qualified_name(&self, plan: logical::PyLogicalPlan) -> Vec<String> {
        let mut qualified_name = match &self.schema_name {
            Some(schema_name) => vec![schema_name.clone()],
            None => vec![],
        };

        match plan.original_plan {
            LogicalPlan::TableScan(table_scan) => {
                qualified_name.push(table_scan.table_name);
            }
            _ => {
                qualified_name.push(self.table_name.clone());
            }
        }

        qualified_name
    }

    #[pyo3(name = "getRowType")]
    pub fn row_type(&self) -> RelDataType {
        let mut fields: Vec<RelDataTypeField> = Vec::new();
        for (name, data_type) in &self.columns {
            fields.push(RelDataTypeField::new(name.as_str(), data_type.clone(), 255));
        }
        RelDataType::new(false, fields)
    }
}

/// Traverses the logical plan to locate the Table associated with the query
pub(crate) fn table_from_logical_plan(
    plan: &LogicalPlan,
) -> Result<Option<DaskTable>, DaskPlannerError> {
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
                    DaskTypeMap::from(
                        SqlTypeName::from_arrow(data_type)?,
                        data_type.clone().into(),
                    ),
                ));
            }

            let table_ref: TableReference = table_scan.table_name.as_str().into();
            let (schema, tbl) = match table_ref {
                TableReference::Bare { table } => ("".to_string(), table),
                TableReference::Partial { schema, table } => (schema.to_string(), table),
                TableReference::Full {
                    catalog: _,
                    schema,
                    table,
                } => (schema.to_string(), table),
            };

            Ok(Some(DaskTable {
                schema_name: Some(schema),
                table_name: String::from(tbl),
                statistics: DaskStatistics { row_count: 0.0 },
                columns: cols,
                filepath: None,
            }))
        }
        LogicalPlan::Join(join) => {
            // TODO: Don't always hardcode the left
            table_from_logical_plan(&join.left)
        }
        LogicalPlan::Aggregate(agg) => table_from_logical_plan(&agg.input),
        LogicalPlan::SubqueryAlias(alias) => table_from_logical_plan(&alias.input),
        LogicalPlan::EmptyRelation(empty_relation) => {
            let fields: &Vec<DFField> = empty_relation.schema.fields();

            let mut cols: Vec<(String, DaskTypeMap)> = Vec::new();
            for field in fields {
                let data_type: &DataType = field.data_type();
                cols.push((
                    String::from(field.name()),
                    DaskTypeMap::from(
                        SqlTypeName::from_arrow(data_type)?,
                        data_type.clone().into(),
                    ),
                ));
            }

            Ok(Some(DaskTable {
                schema_name: Some(String::from("EmptySchema")),
                table_name: String::from("EmptyRelation"),
                statistics: DaskStatistics { row_count: 0.0 },
                columns: cols,
                filepath: None,
            }))
        }
        LogicalPlan::Extension(ex) => {
            let node = ex.node.as_any();
            if let Some(e) = node.downcast_ref::<CreateTablePlanNode>() {
                Ok(Some(DaskTable {
                    schema_name: e.schema_name.clone(),
                    table_name: e.table_name.clone(),
                    statistics: DaskStatistics { row_count: 0.0 },
                    columns: vec![],
                    filepath: None,
                }))
            } else if let Some(e) = node.downcast_ref::<PredictModelPlanNode>() {
                Ok(Some(DaskTable {
                    schema_name: e.schema_name.clone(),
                    table_name: e.model_name.clone(),
                    statistics: DaskStatistics { row_count: 0.0 },
                    columns: vec![],
                    filepath: None,
                }))
            } else {
                Err(DaskPlannerError::Internal(format!(
                    "table_from_logical_plan: unimplemented LogicalPlan type {plan:?} encountered"
                )))
            }
        }
        _ => Err(DaskPlannerError::Internal(format!(
            "table_from_logical_plan: unimplemented LogicalPlan type {plan:?} encountered"
        ))),
    }
}
