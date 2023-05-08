use std::sync::Arc;

use datafusion_python::{
    datafusion_common::DFField,
    datafusion_expr::{expr::Sort, utils::exprlist_to_fields, Expr, LogicalPlan},
    expr::PyExpr,
};
use pyo3::PyResult;

use crate::error::DaskPlannerError;

/// Convert a list of DataFusion Expr to PyExpr
pub fn py_expr_list(_input: &Arc<LogicalPlan>, expr: &[Expr]) -> PyResult<Vec<PyExpr>> {
    Ok(expr.iter().map(|e| PyExpr::from(e.clone())).collect())
}

/// Determines the name of the `Expr` instance by examining the LogicalPlan
pub fn column_name(expr: &Expr, plan: &LogicalPlan) -> Result<String, DaskPlannerError> {
    let field = expr_to_field(expr, plan)?;
    Ok(field.qualified_column().flat_name())
}

/// Create a [DFField] representing an [Expr], given an input [LogicalPlan] to resolve against
pub fn expr_to_field(expr: &Expr, input_plan: &LogicalPlan) -> Result<DFField, DaskPlannerError> {
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
