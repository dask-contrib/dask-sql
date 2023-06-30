use std::collections::HashMap;

use datafusion_python::{
    datafusion::arrow::datatypes::{DataType, TimeUnit},
    datafusion_common::{
        Column,
        DFField,
        ScalarValue,
    },
    datafusion_expr::{
        logical_plan::Filter,
        utils::from_plan,
        BinaryExpr,
        Expr,
        LogicalPlan,
    },
};

// Sometimes, DataFusion's optimizer will raise an OptimizationException before we even get the
// chance to correct it anywhere. In these cases, we can still modify the LogicalPlan as an
// optimizer rule would, however we have to run it independently, separately of DataFusion's
// optimization framework. Ideally, these "pre-optimization" rules aren't performing any complex
// logic, but rather "pre-processing" the LogicalPlan for the optimizer. For example, the
// datetime_coercion preoptimizer rule fixes a bug involving Timestamp-Int operations.

// Helper function for datetime_coercion rule, which returns a vector of columns and literals
// involved in a (possibly nested) BinaryExpr
fn extract_columns_and_literals(expr: &Expr) -> Vec<Expr> {
    let mut result = Vec::new();
    match expr {
        Expr::Column(c) => {
            result.push(Expr::Column(c.clone()));
        }
        Expr::Literal(l) => {
            result.push(Expr::Literal(l.clone()));
        }
        Expr::BinaryExpr(b) => {
            result.extend(extract_columns_and_literals(&b.left));
            result.extend(extract_columns_and_literals(&b.right));
        }
        _ => (),
    }
    result
}

// Helper function for datetime_coercion rule, which uses a LogicalPlan's schema to obtain the
// datatype of a desired column
fn find_data_type(column: Column, fields: Vec<DFField>) -> Option<DataType> {
    for field in fields {
        if let Some(qualifier) = field.qualifier() {
            if qualifier.table() == column.relation.clone().unwrap().table() && field.field().name() == &column.name {
                return Some(field.field().data_type().clone());
            }
        }
    }
    None
}

// Helper function for datetime_coercion rule, which, given a BinaryExpr and a HashMap in which the
// key represents a Literal and the value represents a Literal to replace the key with, returns the
// modified BinaryExpr
fn replace_literals(expr: Expr, replacements: &HashMap<Expr, Expr>) -> Expr {
    match expr {
        Expr::Literal(l) => {
            if let Some(new_literal) = replacements.get(&Expr::Literal(l.clone())) {
                new_literal.clone()
            } else {
                Expr::Literal(l)
            }
        }
        Expr::BinaryExpr(b) => {
            let left = replace_literals(*b.left, replacements);
            let right = replace_literals(*b.right, replacements);
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op: b.op,
                right: Box::new(right),
            })
        }
        _ => expr,
    }
}

// Preoptimization rule which detects when the user is trying to perform a binary operation on a
// datetime and an integer, then converts the integer to a IntervalMonthDayNano. For example, if we
// have a date_col + 5, we assume that we are adding 5 days to the date_col
pub fn datetime_coercion(plan: &LogicalPlan) -> Option<LogicalPlan> {
    match plan {
        LogicalPlan::Filter(f) => {
            let filter_expr = f.predicate.clone();
            let columns_and_literals = extract_columns_and_literals(&filter_expr);

            // Detect whether a timestamp is involved in the operation
            let mut is_timestamp_operation = false;
            for item in columns_and_literals.clone() {
                if let Expr::Column(column) = item {
                    if let Some(data_type) = find_data_type(column, plan.schema().fields().clone()) {
                        match data_type {
                            DataType::Timestamp(TimeUnit::Nanosecond, _) => is_timestamp_operation = true,
                            _ => (),
                        }
                    }
                }
            }

            // Convert an integer to an IntervalMonthDayNano
            let mut days_to_nanoseconds: HashMap<Expr, Expr> = HashMap::new();
            if is_timestamp_operation {
                for item in columns_and_literals {
                    if let Expr::Literal(ScalarValue::Int64(i)) = item {
                        let ns = i.unwrap() as i128 * 18446744073709552000;

                        days_to_nanoseconds.insert(
                            Expr::Literal(ScalarValue::Int64(i)),
                            Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(ns))),
                        );
                    }
                }
            }

            let new_filter = replace_literals(filter_expr, &days_to_nanoseconds);
            Some(LogicalPlan::Filter(Filter::try_new(new_filter, f.input.clone()).unwrap()))
        }
        _ => optimize_children(plan.clone()),
    }
}

// Function used to iterate through a LogicalPlan and update it accordingly
pub fn optimize_children(existing_plan: LogicalPlan) -> Option<LogicalPlan> {
    let plan = existing_plan.clone();
    let new_exprs = plan.expressions();
    let mut new_inputs = Vec::with_capacity(plan.inputs().len());
    let mut plan_is_changed = false;
    for input in plan.inputs() {
        // Since datetime_coercion is the only preoptimizer rule that we have at the moment, we
        // hardcode it here. If additional preoptimizer rules are added in the future, this can be
        // modified
        let new_input = datetime_coercion(input);
        plan_is_changed = plan_is_changed || new_input.is_some();
        new_inputs.push(new_input.unwrap_or_else(|| input.clone()))
    }
    if plan_is_changed {
        Some(from_plan(&plan, &new_exprs, &new_inputs).ok()?)
    } else {
        Some(existing_plan)
    }
}
