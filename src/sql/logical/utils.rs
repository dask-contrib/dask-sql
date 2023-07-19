use std::sync::Arc;

use datafusion_python::{
    datafusion::arrow::datatypes::DataType,
    datafusion_common::{DFField, ScalarValue},
    datafusion_expr::{
        expr::{InList, Sort},
        utils::exprlist_to_fields,
        Cast,
        DdlStatement,
        Expr,
        LogicalPlan,
    },
    expr::{projection::PyProjection, table_scan::PyTableScan, PyExpr},
};
use pyo3::{prelude::*, pyfunction, PyObject, PyResult};

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
        DaskLogicalPlan,
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
pub fn py_column_name(expr: PyExpr, plan: DaskLogicalPlan) -> Result<String> {
    column_name(&expr.expr, &(*plan.plan()).clone())
}

#[pyfunction]
pub fn get_current_node_type(plan: DaskLogicalPlan) -> Result<String> {
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
        LogicalPlan::Ddl(ddl) => match ddl {
            DdlStatement::CreateExternalTable(_) => "CreateExternalTable".to_string(),
            DdlStatement::CreateCatalog(_) => "CreateCatalog".to_string(),
            DdlStatement::CreateCatalogSchema(_) => "CreateCatalogSchema".to_string(),
            DdlStatement::CreateMemoryTable(_) => "CreateMemoryTable".to_string(),
            DdlStatement::CreateView(_) => "CreateView".to_string(),
            DdlStatement::DropCatalogSchema(_) => "DropCatalogSchema".to_string(),
            DdlStatement::DropTable(_) => "DropTable".to_string(),
            DdlStatement::DropView(_) => "DropView".to_string(),
        },
        LogicalPlan::Values(_values) => "Values".to_string(),
        LogicalPlan::Explain(_explain) => "Explain".to_string(),
        LogicalPlan::Analyze(_analyze) => "Analyze".to_string(),
        LogicalPlan::Subquery(_sub_query) => "Subquery".to_string(),
        LogicalPlan::SubqueryAlias(_sqalias) => "SubqueryAlias".to_string(),
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
pub fn plan_to_table(plan: DaskLogicalPlan) -> PyResult<DaskTable> {
    match table_from_logical_plan(&plan.plan())? {
        Some(table) => Ok(table),
        None => Err(py_type_err(
            "Unable to compute DaskTable from DataFusion LogicalPlan",
        )),
    }
}

#[pyfunction]
pub fn row_type(plan: DaskLogicalPlan) -> PyResult<RelDataType> {
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

#[pyfunction]
pub fn distinct_agg(expr: PyExpr) -> PyResult<bool> {
    match expr.expr {
        Expr::AggregateFunction(funct) => Ok(funct.distinct),
        Expr::AggregateUDF { .. } => Ok(false),
        Expr::Alias(expr, _) => match expr.as_ref() {
            Expr::AggregateFunction(funct) => Ok(funct.distinct),
            Expr::AggregateUDF { .. } => Ok(false),
            _ => Err(py_type_err(
                "isDistinctAgg() - Non-aggregate expression encountered",
            )),
        },
        _ => Err(py_type_err(
            "getFilterExpr() - Non-aggregate expression encountered",
        )),
    }
}

/// Returns if a sort expressions is an ascending sort
#[pyfunction]
pub fn sort_ascending(expr: PyExpr) -> PyResult<bool> {
    match expr.expr {
        Expr::Sort(Sort { asc, .. }) => Ok(asc),
        _ => Err(py_type_err(format!(
            "Provided Expr {:?} is not a sort type",
            &expr.expr
        ))),
    }
}

/// Returns if nulls should be placed first in a sort expression
#[pyfunction]
pub fn sort_nulls_first(expr: PyExpr) -> PyResult<bool> {
    match expr.expr {
        Expr::Sort(Sort { nulls_first, .. }) => Ok(nulls_first),
        _ => Err(py_type_err(format!(
            "Provided Expr {:?} is not a sort type",
            &expr.expr
        ))),
    }
}

#[pyfunction]
pub fn get_filter_expr(expr: PyExpr) -> PyResult<Option<PyExpr>> {
    // TODO refactor to avoid duplication
    match &expr.expr {
        Expr::Alias(expr, _) => match expr.as_ref() {
            Expr::AggregateFunction(agg_function) => match &agg_function.filter {
                Some(filter) => Ok(Some(PyExpr::from(*filter.clone()))),
                None => Ok(None),
            },
            Expr::AggregateUDF(filter) => match &filter.filter {
                Some(filter) => Ok(Some(PyExpr::from(*filter.clone()))),
                None => Ok(None),
            },
            _ => Err(py_type_err(
                "get_filter_expr() - Non-aggregate expression encountered",
            )),
        },
        Expr::AggregateFunction(agg_function) => match &agg_function.filter {
            Some(filter) => Ok(Some(PyExpr::from(*filter.clone()))),
            None => Ok(None),
        },
        Expr::AggregateUDF(filter, ..) => match &filter.filter {
            Some(filter) => Ok(Some(PyExpr::from(*filter.clone()))),
            None => Ok(None),
        },
        _ => Err(py_type_err(
            "get_filter_expr() - Non-aggregate expression encountered",
        )),
    }
}

#[pyfunction]
pub fn get_precision_scale(expr: PyExpr) -> PyResult<(u8, i8)> {
    Ok(match &expr.expr {
        Expr::Cast(Cast { expr: _, data_type }) => match data_type {
            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                (*precision, *scale)
            }
            _ => {
                return Err(py_type_err(format!(
                    "Catch all triggered for Cast in get_precision_scale; {data_type:?}"
                )))
            }
        },
        _ => {
            return Err(py_type_err(format!(
                "Catch all triggered in get_precision_scale; {:?}",
                &expr.expr
            )))
        }
    })
}

type FilterTuple = (String, String, Option<Vec<PyObject>>);
#[pyclass(name = "FilteredResult", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct PyFilteredResult {
    // Certain Expr(s) do not have supporting logic in pyarrow for IO filtering
    // at read time. Those Expr(s) cannot be ignored however. This field stores
    // those Expr(s) so that they can be used on the Python side to create
    // Dask operations that handle that filtering as an extra task in the graph.
    #[pyo3(get)]
    pub io_unfilterable_exprs: Vec<PyExpr>,
    // Expr(s) that can have their filtering logic performed in the pyarrow IO logic
    // are stored here in a DNF format that is expected by pyarrow.
    #[pyo3(get)]
    pub filtered_exprs: Vec<(PyExpr, FilterTuple)>,
}

#[pyfunction]
pub fn get_table_scan_dnf_filters(
    table_scan: PyTableScan,
    py: Python,
) -> PyResult<PyFilteredResult> {
    let results = self::_expand_dnf_filters(&table_scan.table_scan.filters, py);
    Ok(results)
}

/// Ensures that a valid Expr variant type is present
fn _valid_expr_type(expr: &[Expr]) -> bool {
    expr.iter()
        .all(|f| matches!(f, Expr::Column(_) | Expr::Literal(_)))
}

/// Transform the singular Expr instance into its DNF form serialized in a Vec instance. Possibly recursively expanding
/// it as well if needed.
pub fn _expand_dnf_filter(filter: &Expr, py: Python) -> Result<Vec<(PyExpr, FilterTuple)>> {
    let mut filter_tuple: Vec<(PyExpr, FilterTuple)> = Vec::new();

    match filter {
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            // Only handle simple Expr(s) for InList operations for now
            if self::_valid_expr_type(list) {
                // While ANSI SQL would not allow for anything other than a Column or Literal
                // value in this "identifying" `expr` we explicitly check that here just to be sure.
                // IF it is something else it is returned to Dask to handle
                let ident = match *expr.clone() {
                    Expr::Column(col) => Ok(col.name),
                    Expr::Alias(_, name) => Ok(name),
                    Expr::Literal(val) => Ok(format!("{}", val)),
                    _ => Err(DaskPlannerError::InvalidIOFilter(format!(
                        "Invalid InList Expr type `{}`. using in Dask instead",
                        filter
                    ))),
                };

                let op = if *negated { "not in" } else { "in" };
                let il: Result<Vec<PyObject>> = list
                    .iter()
                    .map(|f| match f {
                        Expr::Column(col) => Ok(col.name.clone().into_py(py)),
                        Expr::Alias(_, name) => Ok(name.clone().into_py(py)),
                        Expr::Literal(val) => match val {
                            ScalarValue::Boolean(val) => Ok(val.unwrap().into_py(py)),
                            ScalarValue::Float32(val) => Ok(val.unwrap().into_py(py)),
                            ScalarValue::Float64(val) => Ok(val.unwrap().into_py(py)),
                            ScalarValue::Int8(val) => Ok(val.unwrap().into_py(py)),
                            ScalarValue::Int16(val) => Ok(val.unwrap().into_py(py)),
                            ScalarValue::Int32(val) => Ok(val.unwrap().into_py(py)),
                            ScalarValue::Int64(val) => Ok(val.unwrap().into_py(py)),
                            ScalarValue::UInt8(val) => Ok(val.unwrap().into_py(py)),
                            ScalarValue::UInt16(val) => Ok(val.unwrap().into_py(py)),
                            ScalarValue::UInt32(val) => Ok(val.unwrap().into_py(py)),
                            ScalarValue::UInt64(val) => Ok(val.unwrap().into_py(py)),
                            ScalarValue::Utf8(val) => Ok(val.clone().unwrap().into_py(py)),
                            ScalarValue::LargeUtf8(val) => Ok(val.clone().unwrap().into_py(py)),
                            _ => Err(DaskPlannerError::InvalidIOFilter(format!(
                                "Unsupported ScalarValue `{}` encountered. using in Dask instead",
                                filter
                            ))),
                        },
                        _ => Ok(f.canonical_name().into_py(py)),
                    })
                    .collect();

                filter_tuple.push((
                    PyExpr::from(filter.clone()),
                    (
                        ident.unwrap_or(expr.canonical_name()),
                        op.to_string(),
                        Some(il?),
                    ),
                ));
                Ok(filter_tuple)
            } else {
                let er = DaskPlannerError::InvalidIOFilter(format!(
                    "Invalid identifying column Expr instance `{}`. using in Dask instead",
                    filter
                ));
                Err::<Vec<(PyExpr, FilterTuple)>, DaskPlannerError>(er)
            }
        }
        Expr::IsNotNull(expr) => {
            // Only handle simple Expr(s) for IsNotNull operations for now
            let ident = match *expr.clone() {
                Expr::Column(col) => Ok(col.name),
                _ => Err(DaskPlannerError::InvalidIOFilter(format!(
                    "Invalid IsNotNull Expr type `{}`. using in Dask instead",
                    filter
                ))),
            };

            filter_tuple.push((
                PyExpr::from(filter.clone()),
                (
                    ident.unwrap_or(expr.canonical_name()),
                    "is not".to_string(),
                    None,
                ),
            ));
            Ok(filter_tuple)
        }
        _ => {
            let er = DaskPlannerError::InvalidIOFilter(format!(
                "Unable to apply filter: `{}` to IO reader, using in Dask instead",
                filter
            ));
            Err::<Vec<(PyExpr, FilterTuple)>, DaskPlannerError>(er)
        }
    }
}

/// Consume the `TableScan` filters (Expr(s)) and convert them into a PyArrow understandable
/// DNF format that can be directly passed to PyArrow IO readers for Predicate Pushdown. Expr(s)
/// that cannot be converted to correlating PyArrow IO calls will be returned as is and can be
/// used in the Python logic to form Dask tasks for the graph to do computational filtering.
pub fn _expand_dnf_filters(filters: &[Expr], py: Python) -> PyFilteredResult {
    let mut filtered_exprs: Vec<(PyExpr, FilterTuple)> = Vec::new();
    let mut unfiltered_exprs: Vec<PyExpr> = Vec::new();

    filters
        .iter()
        .for_each(|f| match self::_expand_dnf_filter(f, py) {
            Ok(mut expanded_dnf_filter) => filtered_exprs.append(&mut expanded_dnf_filter),
            Err(_e) => unfiltered_exprs.push(PyExpr::from(f.clone())),
        });

    PyFilteredResult {
        io_unfilterable_exprs: unfiltered_exprs,
        filtered_exprs,
    }
}
