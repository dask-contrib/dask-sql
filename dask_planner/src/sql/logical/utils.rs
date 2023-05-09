use std::sync::Arc;

use datafusion_python::{
    datafusion_common::DFField,
    datafusion_expr::{expr::Sort, utils::exprlist_to_fields, Expr, LogicalPlan},
    expr::{projection::PyProjection, PyExpr},
    sql::logical::PyLogicalPlan,
};
use pyo3::{pyfunction, PyResult};

use super::{
    alter_schema::AlterSchemaPlanNode,
    alter_table::AlterTablePlanNode,
    analyze_table::AnalyzeTablePlanNode,
    create_catalog_schema::CreateCatalogSchemaPlanNode,
    create_experiment::CreateExperimentPlanNode,
    create_model::CreateModelPlanNode,
    create_table::CreateTablePlanNode,
    describe_model::DescribeModelPlanNode,
    drop_model::DropModelPlanNode,
    drop_schema::DropSchemaPlanNode,
    export_model::ExportModelPlanNode,
    predict_model::PredictModelPlanNode,
    show_columns::ShowColumnsPlanNode,
    show_models::ShowModelsPlanNode,
    show_schemas::ShowSchemasPlanNode,
    show_tables::ShowTablesPlanNode,
    use_schema::UseSchemaPlanNode,
};
use crate::{
    error::{DaskPlannerError, Result},
    sql::{
        exceptions::py_type_err,
        table::{table_from_logical_plan, DaskTable},
        types::{rel_data_type::RelDataType, rel_data_type_field::RelDataTypeField},
    },
};

/// Convert a list of DataFusion Expr to PyExpr
pub fn py_expr_list(_input: &Arc<LogicalPlan>, expr: &[Expr]) -> PyResult<Vec<PyExpr>> {
    Ok(expr.iter().map(|e| PyExpr::from(e.clone())).collect())
}

/// Determines the name of the `Expr` instance by examining the LogicalPlan
pub fn column_name(expr: &Expr, plan: &LogicalPlan) -> Result<String> {
    let field = expr_to_field(expr, plan)?;
    Ok(field.qualified_column().flat_name())
}

/// Create a [DFField] representing an [Expr], given an input [LogicalPlan] to resolve against
pub fn expr_to_field(expr: &Expr, input_plan: &LogicalPlan) -> Result<DFField> {
    match expr {
        Expr::Sort(Sort { expr, .. }) => {
            // DataFusion does not support create_name for sort expressions (since they never
            // appear in projections) so we just delegate to the contained expression instead
            expr_to_field(expr, input_plan)
        }
        _ => {
            let fields =
                exprlist_to_fields(&[expr.clone()], input_plan).map_err(DaskPlannerError::from)?;
            Ok(fields[0].clone())
        }
    }
}

#[pyfunction]
pub fn get_current_node_type(plan: PyLogicalPlan) -> Result<String> {
    Ok(match &*plan.plan() {
        LogicalPlan::Dml(_) => "DataManipulationLanguage".to_string(),
        LogicalPlan::DescribeTable(_) => "DescribeTable".to_string(),
        LogicalPlan::Prepare(_) => "Prepare".to_string(),
        LogicalPlan::Distinct(_) => "Distinct".to_string(),
        LogicalPlan::Projection(_projection) => "Projection".to_string(),
        LogicalPlan::Filter(_filter) => "Filter".to_string(),
        LogicalPlan::Window(_window) => "Window".to_string(),
        LogicalPlan::Aggregate(_aggregate) => "Aggregate".to_string(),
        LogicalPlan::Sort(_sort) => "Sort".to_string(),
        LogicalPlan::Join(_join) => "Join".to_string(),
        LogicalPlan::CrossJoin(_cross_join) => "CrossJoin".to_string(),
        LogicalPlan::Repartition(_repartition) => "Repartition".to_string(),
        LogicalPlan::Union(_union) => "Union".to_string(),
        LogicalPlan::TableScan(_table_scan) => "TableScan".to_string(),
        LogicalPlan::EmptyRelation(_empty_relation) => "EmptyRelation".to_string(),
        LogicalPlan::Limit(_limit) => "Limit".to_string(),
        LogicalPlan::CreateExternalTable(_create_external_table) => {
            "CreateExternalTable".to_string()
        }
        LogicalPlan::CreateMemoryTable(_create_memory_table) => "CreateMemoryTable".to_string(),
        LogicalPlan::DropTable(_drop_table) => "DropTable".to_string(),
        LogicalPlan::DropView(_drop_view) => "DropView".to_string(),
        LogicalPlan::Values(_values) => "Values".to_string(),
        LogicalPlan::Explain(_explain) => "Explain".to_string(),
        LogicalPlan::Analyze(_analyze) => "Analyze".to_string(),
        LogicalPlan::Subquery(_sub_query) => "Subquery".to_string(),
        LogicalPlan::SubqueryAlias(_sqalias) => "SubqueryAlias".to_string(),
        LogicalPlan::CreateCatalogSchema(_create) => "CreateCatalogSchema".to_string(),
        LogicalPlan::CreateCatalog(_create_catalog) => "CreateCatalog".to_string(),
        LogicalPlan::CreateView(_create_view) => "CreateView".to_string(),
        LogicalPlan::Statement(_) => "Statement".to_string(),
        // Further examine and return the name that is a possible Dask-SQL Extension type
        LogicalPlan::Extension(extension) => {
            let node = extension.node.as_any();
            if node.downcast_ref::<CreateModelPlanNode>().is_some() {
                "CreateModel".to_string()
            } else if node.downcast_ref::<CreateExperimentPlanNode>().is_some() {
                "CreateExperiment".to_string()
            } else if node.downcast_ref::<CreateCatalogSchemaPlanNode>().is_some() {
                "CreateCatalogSchema".to_string()
            } else if node.downcast_ref::<CreateTablePlanNode>().is_some() {
                "CreateTable".to_string()
            } else if node.downcast_ref::<DropModelPlanNode>().is_some() {
                "DropModel".to_string()
            } else if node.downcast_ref::<PredictModelPlanNode>().is_some() {
                "PredictModel".to_string()
            } else if node.downcast_ref::<ExportModelPlanNode>().is_some() {
                "ExportModel".to_string()
            } else if node.downcast_ref::<DescribeModelPlanNode>().is_some() {
                "DescribeModel".to_string()
            } else if node.downcast_ref::<ShowSchemasPlanNode>().is_some() {
                "ShowSchemas".to_string()
            } else if node.downcast_ref::<ShowTablesPlanNode>().is_some() {
                "ShowTables".to_string()
            } else if node.downcast_ref::<ShowColumnsPlanNode>().is_some() {
                "ShowColumns".to_string()
            } else if node.downcast_ref::<ShowModelsPlanNode>().is_some() {
                "ShowModels".to_string()
            } else if node.downcast_ref::<DropSchemaPlanNode>().is_some() {
                "DropSchema".to_string()
            } else if node.downcast_ref::<UseSchemaPlanNode>().is_some() {
                "UseSchema".to_string()
            } else if node.downcast_ref::<AnalyzeTablePlanNode>().is_some() {
                "AnalyzeTable".to_string()
            } else if node.downcast_ref::<AlterTablePlanNode>().is_some() {
                "AlterTable".to_string()
            } else if node.downcast_ref::<AlterSchemaPlanNode>().is_some() {
                "AlterSchema".to_string()
            } else {
                // Default to generic `Extension`
                "Extension".to_string()
            }
        }
        LogicalPlan::Unnest(_unnest) => "Unnest".to_string(),
    })
}

#[pyfunction]
pub fn plan_to_table(plan: PyLogicalPlan) -> PyResult<DaskTable> {
    match table_from_logical_plan(&plan.plan())? {
        Some(table) => Ok(table),
        None => Err(py_type_err(
            "Unable to compute DaskTable from DataFusion LogicalPlan",
        )),
    }
}

#[pyfunction]
pub fn row_type(plan: PyLogicalPlan) -> PyResult<RelDataType> {
    match &*plan.plan() {
        LogicalPlan::Join(join) => {
            let mut lhs_fields: Vec<RelDataTypeField> = join
                .left
                .schema()
                .fields()
                .iter()
                .map(|f| RelDataTypeField::from(f, join.left.schema().as_ref()))
                .collect::<Result<Vec<_>>>()
                .map_err(py_type_err)?;

            let mut rhs_fields: Vec<RelDataTypeField> = join
                .right
                .schema()
                .fields()
                .iter()
                .map(|f| RelDataTypeField::from(f, join.right.schema().as_ref()))
                .collect::<Result<Vec<_>>>()
                .map_err(py_type_err)?;

            lhs_fields.append(&mut rhs_fields);
            Ok(RelDataType::new(false, lhs_fields))
        }
        LogicalPlan::Distinct(distinct) => {
            let schema = distinct.input.schema();
            let rel_fields: Vec<RelDataTypeField> = schema
                .fields()
                .iter()
                .map(|f| RelDataTypeField::from(f, schema.as_ref()))
                .collect::<Result<Vec<_>>>()
                .map_err(py_type_err)?;
            Ok(RelDataType::new(false, rel_fields))
        }
        _ => {
            let plan = (*plan.plan()).clone();
            let schema = plan.schema();
            let rel_fields: Vec<RelDataTypeField> = schema
                .fields()
                .iter()
                .map(|f| RelDataTypeField::from(f, schema.as_ref()))
                .collect::<Result<Vec<_>>>()
                .map_err(py_type_err)?;

            Ok(RelDataType::new(false, rel_fields))
        }
    }
}

#[pyfunction]
pub fn named_projects(projection: PyProjection) -> PyResult<Vec<(String, PyExpr)>> {
    let mut named: Vec<(String, PyExpr)> = Vec::new();
    for expression in projection.projection.expr {
        let py_expr: PyExpr = PyExpr::from(expression);
        for expr in PyProjection::projected_expressions(&py_expr) {
            match expr.expr {
                Expr::Alias(ex, name) => named.push((name.to_string(), PyExpr::from(*ex))),
                _ => {
                    if let Ok(name) = column_name(&expr.expr, &projection.projection.input) {
                        named.push((name, expr.clone()));
                    }
                }
            }
        }
    }
    Ok(named)
}
