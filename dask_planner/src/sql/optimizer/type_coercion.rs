// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Optimizer rule for type validation and coercion

use std::sync::Arc;

use arrow::datatypes::{DataType, IntervalUnit};
use datafusion_common::{
    parse_interval,
    DFSchema,
    DFSchemaRef,
    DataFusionError,
    Result,
    ScalarValue,
};
use datafusion_expr::{
    aggregate_function,
    expr::{Between, BinaryExpr, Case, Like},
    expr_rewriter::{ExprRewriter, RewriteRecursion},
    function,
    is_false,
    is_not_false,
    is_not_true,
    is_not_unknown,
    is_true,
    is_unknown,
    logical_plan::Subquery,
    type_coercion,
    type_coercion::{
        binary::{coerce_types, comparison_coercion},
        functions::data_types,
        is_date,
        is_numeric,
        is_timestamp,
        other::{get_coerce_type_for_case_when, get_coerce_type_for_list},
    },
    utils::from_plan,
    AggregateFunction,
    Expr,
    ExprSchemable,
    LogicalPlan,
    Operator,
    Signature,
    WindowFrame,
    WindowFrameBound,
    WindowFrameUnits,
};
use datafusion_optimizer::{utils::rewrite_preserving_name, OptimizerConfig, OptimizerRule};

#[derive(Default)]
pub struct TypeCoercion {}

// impl TypeCoercion {
//     pub fn new() -> Self {
//         Self {}
//     }
// }

impl OptimizerRule for TypeCoercion {
    fn name(&self) -> &str {
        "type_coercion"
    }

    fn optimize(
        &self,
        plan: &LogicalPlan,
        _optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        optimize_internal(&DFSchema::empty(), plan)
    }
}

fn optimize_internal(
    // use the external schema to handle the correlated subqueries case
    external_schema: &DFSchema,
    plan: &LogicalPlan,
) -> Result<LogicalPlan> {
    // optimize child plans first
    let new_inputs = plan
        .inputs()
        .iter()
        .map(|p| optimize_internal(external_schema, p))
        .collect::<Result<Vec<_>>>()?;
    // get schema representing all available input fields. This is used for data type
    // resolution only, so order does not matter here
    let mut schema =
        new_inputs
            .iter()
            .map(|input| input.schema())
            .fold(DFSchema::empty(), |mut lhs, rhs| {
                lhs.merge(rhs);
                lhs
            });

    // merge the outer schema for correlated subqueries
    // like case:
    // select t2.c2 from t1 where t1.c1 in (select t2.c1 from t2 where t2.c2=t1.c3)
    schema.merge(external_schema);

    let mut expr_rewrite = TypeCoercionRewriter {
        schema: Arc::new(schema),
    };

    let new_expr = plan
        .expressions()
        .into_iter()
        .map(|expr| {
            // ensure aggregate names don't change:
            // https://github.com/apache/arrow-datafusion/issues/3555
            rewrite_preserving_name(expr, &mut expr_rewrite)
        })
        .collect::<Result<Vec<_>>>()?;

    from_plan(plan, &new_expr, &new_inputs)
}

pub(crate) struct TypeCoercionRewriter {
    pub(crate) schema: DFSchemaRef,
}

impl ExprRewriter for TypeCoercionRewriter {
    fn pre_visit(&mut self, _expr: &Expr) -> Result<RewriteRecursion> {
        Ok(RewriteRecursion::Continue)
    }

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        match expr {
            Expr::ScalarSubquery(Subquery { subquery }) => {
                let new_plan = optimize_internal(&self.schema, &subquery)?;
                Ok(Expr::ScalarSubquery(Subquery::new(new_plan)))
            }
            Expr::Exists { subquery, negated } => {
                let new_plan = optimize_internal(&self.schema, &subquery.subquery)?;
                Ok(Expr::Exists {
                    subquery: Subquery::new(new_plan),
                    negated,
                })
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let new_plan = optimize_internal(&self.schema, &subquery.subquery)?;
                Ok(Expr::InSubquery {
                    expr,
                    subquery: Subquery::new(new_plan),
                    negated,
                })
            }
            Expr::IsTrue(expr) => {
                let expr = is_true(get_casted_expr_for_bool_op(&expr, &self.schema)?);
                Ok(expr)
            }
            Expr::IsNotTrue(expr) => {
                let expr = is_not_true(get_casted_expr_for_bool_op(&expr, &self.schema)?);
                Ok(expr)
            }
            Expr::IsFalse(expr) => {
                let expr = is_false(get_casted_expr_for_bool_op(&expr, &self.schema)?);
                Ok(expr)
            }
            Expr::IsNotFalse(expr) => {
                let expr = is_not_false(get_casted_expr_for_bool_op(&expr, &self.schema)?);
                Ok(expr)
            }
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }) => {
                let left_type = expr.get_type(&self.schema)?;
                let right_type = pattern.get_type(&self.schema)?;
                let coerced_type = coerce_types(&left_type, &Operator::Like, &right_type)?;
                let expr = Box::new(expr.cast_to(&coerced_type, &self.schema)?);
                let pattern = Box::new(pattern.cast_to(&coerced_type, &self.schema)?);
                let expr = Expr::Like(Like::new(negated, expr, pattern, escape_char));
                Ok(expr)
            }
            Expr::ILike(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }) => {
                let left_type = expr.get_type(&self.schema)?;
                let right_type = pattern.get_type(&self.schema)?;
                let coerced_type = coerce_types(&left_type, &Operator::Like, &right_type)?;
                let expr = Box::new(expr.cast_to(&coerced_type, &self.schema)?);
                let pattern = Box::new(pattern.cast_to(&coerced_type, &self.schema)?);
                let expr = Expr::ILike(Like::new(negated, expr, pattern, escape_char));
                Ok(expr)
            }
            Expr::IsUnknown(expr) => {
                // will convert the binary(expr,IsNotDistinctFrom,lit(Boolean(None));
                let left_type = expr.get_type(&self.schema)?;
                let right_type = DataType::Boolean;
                let coerced_type =
                    coerce_types(&left_type, &Operator::IsNotDistinctFrom, &right_type)?;
                let expr = is_unknown(expr.cast_to(&coerced_type, &self.schema)?);
                Ok(expr)
            }
            Expr::IsNotUnknown(expr) => {
                // will convert the binary(expr,IsDistinctFrom,lit(Boolean(None));
                let left_type = expr.get_type(&self.schema)?;
                let right_type = DataType::Boolean;
                let coerced_type =
                    coerce_types(&left_type, &Operator::IsDistinctFrom, &right_type)?;
                let expr = is_not_unknown(expr.cast_to(&coerced_type, &self.schema)?);
                Ok(expr)
            }
            Expr::BinaryExpr(BinaryExpr {
                ref left,
                op,
                ref right,
            }) => {
                let left_type = left.get_type(&self.schema)?;
                let right_type = right.get_type(&self.schema)?;
                match (&left_type, &right_type) {
                    (
                        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _),
                        &DataType::Interval(_),
                    ) => {
                        // this is a workaround for https://github.com/apache/arrow-datafusion/issues/3419
                        Ok(expr.clone())
                    }
                    _ => {
                        let coerced_type = coerce_types(&left_type, &op, &right_type)?;
                        let expr = Expr::BinaryExpr(BinaryExpr::new(
                            Box::new(left.clone().cast_to(&coerced_type, &self.schema)?),
                            op,
                            Box::new(right.clone().cast_to(&coerced_type, &self.schema)?),
                        ));
                        Ok(expr)
                    }
                }
            }
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
                let expr_type = expr.get_type(&self.schema)?;
                let low_type = low.get_type(&self.schema)?;
                let low_coerced_type =
                    comparison_coercion(&expr_type, &low_type).ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Failed to coerce types {} and {} in BETWEEN expression",
                            expr_type, low_type
                        ))
                    })?;
                let high_type = high.get_type(&self.schema)?;
                let high_coerced_type =
                    comparison_coercion(&expr_type, &low_type).ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Failed to coerce types {} and {} in BETWEEN expression",
                            expr_type, high_type
                        ))
                    })?;
                let coercion_type = comparison_coercion(&low_coerced_type, &high_coerced_type)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Failed to coerce types {} and {} in BETWEEN expression",
                            expr_type, high_type
                        ))
                    })?;
                let expr = Expr::Between(Between::new(
                    Box::new(expr.cast_to(&coercion_type, &self.schema)?),
                    negated,
                    Box::new(low.cast_to(&coercion_type, &self.schema)?),
                    Box::new(high.cast_to(&coercion_type, &self.schema)?),
                ));
                Ok(expr)
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let expr_data_type = expr.get_type(&self.schema)?;
                let list_data_types = list
                    .iter()
                    .map(|list_expr| list_expr.get_type(&self.schema))
                    .collect::<Result<Vec<_>>>()?;
                let result_type = get_coerce_type_for_list(&expr_data_type, &list_data_types);
                match result_type {
                    None => Err(DataFusionError::Plan(format!(
                        "Can not find compatible types to compare {:?} with {:?}",
                        expr_data_type, list_data_types
                    ))),
                    Some(coerced_type) => {
                        // find the coerced type
                        let cast_expr = expr.cast_to(&coerced_type, &self.schema)?;
                        let cast_list_expr = list
                            .into_iter()
                            .map(|list_expr| list_expr.cast_to(&coerced_type, &self.schema))
                            .collect::<Result<Vec<_>>>()?;
                        let expr = Expr::InList {
                            expr: Box::new(cast_expr),
                            list: cast_list_expr,
                            negated,
                        };
                        Ok(expr)
                    }
                }
            }
            Expr::Case(case) => {
                // all the result of then and else should be convert to a common data type,
                // if they can be coercible to a common data type, return error.
                let then_types = case
                    .when_then_expr
                    .iter()
                    .map(|when_then| when_then.1.get_type(&self.schema))
                    .collect::<Result<Vec<_>>>()?;
                let else_type = match &case.else_expr {
                    None => Ok(None),
                    Some(expr) => expr.get_type(&self.schema).map(Some),
                }?;
                let case_when_coerce_type = get_coerce_type_for_case_when(&then_types, &else_type);
                match case_when_coerce_type {
                    None => Err(DataFusionError::Internal(format!(
                        "Failed to coerce then ({:?}) and else ({:?}) to common types in CASE WHEN expression",
                        then_types, else_type
                    ))),
                    Some(data_type) => {
                        let left = case.when_then_expr
                            .into_iter()
                            .map(|(when, then)| {
                                let then = then.cast_to(&data_type, &self.schema)?;
                                Ok((when, Box::new(then)))
                            })
                            .collect::<Result<Vec<_>>>()?;
                        let right = match &case.else_expr {
                            None => None,
                            Some(expr) => {
                                Some(Box::new(expr.clone().cast_to(&data_type, &self.schema)?))
                            }
                        };
                        Ok(Expr::Case(Case::new(case.expr,left,right)))
                    }
                }
            }
            Expr::ScalarUDF { fun, args } => {
                let new_expr =
                    coerce_arguments_for_signature(args.as_slice(), &self.schema, &fun.signature)?;
                let expr = Expr::ScalarUDF {
                    fun,
                    args: new_expr,
                };
                Ok(expr)
            }
            Expr::ScalarFunction { fun, args } => {
                let nex_expr = coerce_arguments_for_signature(
                    args.as_slice(),
                    &self.schema,
                    &function::signature(&fun),
                )?;
                let expr = Expr::ScalarFunction {
                    fun,
                    args: nex_expr,
                };
                Ok(expr)
            }
            Expr::AggregateFunction {
                fun,
                args,
                distinct,
                filter,
            } => {
                let new_expr = coerce_agg_exprs_for_signature(
                    &fun,
                    &args,
                    &self.schema,
                    &aggregate_function::signature(&fun),
                )?;
                let expr = Expr::AggregateFunction {
                    fun,
                    args: new_expr,
                    distinct,
                    filter,
                };
                Ok(expr)
            }
            Expr::AggregateUDF { fun, args, filter } => {
                let new_expr =
                    coerce_arguments_for_signature(args.as_slice(), &self.schema, &fun.signature)?;
                let expr = Expr::AggregateUDF {
                    fun,
                    args: new_expr,
                    filter,
                };
                Ok(expr)
            }
            Expr::WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
            } => {
                let window_frame = get_coerced_window_frame(window_frame, &self.schema, &order_by)?;
                let expr = Expr::WindowFunction {
                    fun,
                    args,
                    partition_by,
                    order_by,
                    window_frame,
                };
                Ok(expr)
            }
            expr => Ok(expr),
        }
    }
}

/// Casts the ScalarValue `value` to coerced type.
// When coerced type is `Interval` we use `parse_interval` since `try_from_string` not
// supports conversion from string to Interval
fn convert_to_coerced_type(coerced_type: &DataType, value: &ScalarValue) -> Result<ScalarValue> {
    match value {
        // In here we do casting either for ScalarValue::Utf8(None) or
        // ScalarValue::Utf8(Some(val)). The other types are already casted.
        // The reason is that we convert the sqlparser result
        // to the Utf8 for all possible cases. Hence the types other than Utf8
        // are already casted to appropriate type. Therefore they can be returned directly.
        ScalarValue::Utf8(None) => ScalarValue::try_from(coerced_type),
        ScalarValue::Utf8(Some(val)) => {
            // we need special handling for Interval types
            if let DataType::Interval(..) = coerced_type {
                parse_interval("millisecond", val)
            } else {
                ScalarValue::try_from_string(val.clone(), coerced_type)
            }
        }
        s => Ok(s.clone()),
    }
}

fn coerce_frame_bound(
    coerced_type: &DataType,
    bound: &WindowFrameBound,
) -> Result<WindowFrameBound> {
    Ok(match bound {
        WindowFrameBound::Preceding(val) => {
            WindowFrameBound::Preceding(convert_to_coerced_type(coerced_type, val)?)
        }
        WindowFrameBound::CurrentRow => WindowFrameBound::CurrentRow,
        WindowFrameBound::Following(val) => {
            WindowFrameBound::Following(convert_to_coerced_type(coerced_type, val)?)
        }
    })
}

fn get_coerced_window_frame(
    window_frame: Option<WindowFrame>,
    schema: &DFSchemaRef,
    expressions: &[Expr],
) -> Result<Option<WindowFrame>> {
    fn get_coerced_type(column_type: &DataType) -> Result<DataType> {
        if is_numeric(column_type) {
            Ok(column_type.clone())
        } else if is_timestamp(column_type) || is_date(column_type) {
            Ok(DataType::Interval(IntervalUnit::MonthDayNano))
        } else {
            Err(DataFusionError::Internal(format!(
                "Cannot run range queries on datatype: {:?}",
                column_type
            )))
        }
    }

    if let Some(window_frame) = window_frame {
        let mut window_frame = window_frame;
        let current_types = expressions
            .iter()
            .map(|e| e.get_type(schema))
            .collect::<Result<Vec<_>>>()?;
        match &mut window_frame.units {
            WindowFrameUnits::Range => {
                let col_type = current_types.first().ok_or_else(|| {
                    DataFusionError::Internal("ORDER BY column cannot be empty".to_string())
                })?;
                let coerced_type = get_coerced_type(col_type)?;
                window_frame.start_bound =
                    coerce_frame_bound(&coerced_type, &window_frame.start_bound)?;
                window_frame.end_bound =
                    coerce_frame_bound(&coerced_type, &window_frame.end_bound)?;
            }
            WindowFrameUnits::Rows | WindowFrameUnits::Groups => {
                let coerced_type = DataType::UInt64;
                window_frame.start_bound =
                    coerce_frame_bound(&coerced_type, &window_frame.start_bound)?;
                window_frame.end_bound =
                    coerce_frame_bound(&coerced_type, &window_frame.end_bound)?;
            }
        }

        Ok(Some(window_frame))
    } else {
        Ok(None)
    }
}
// Support the `IsTrue` `IsNotTrue` `IsFalse` `IsNotFalse` type coercion.
// The above op will be rewrite to the binary op when creating the physical op.
fn get_casted_expr_for_bool_op(expr: &Expr, schema: &DFSchemaRef) -> Result<Expr> {
    let left_type = expr.get_type(schema)?;
    let right_type = DataType::Boolean;
    let coerced_type = coerce_types(&left_type, &Operator::IsDistinctFrom, &right_type)?;
    expr.clone().cast_to(&coerced_type, schema)
}

/// Returns `expressions` coerced to types compatible with
/// `signature`, if possible.
///
/// See the module level documentation for more detail on coercion.
fn coerce_arguments_for_signature(
    expressions: &[Expr],
    schema: &DFSchema,
    signature: &Signature,
) -> Result<Vec<Expr>> {
    if expressions.is_empty() {
        return Ok(vec![]);
    }

    let current_types = expressions
        .iter()
        .map(|e| e.get_type(schema))
        .collect::<Result<Vec<_>>>()?;

    let new_types = data_types(&current_types, signature)?;

    expressions
        .iter()
        .enumerate()
        .map(|(i, expr)| expr.clone().cast_to(&new_types[i], schema))
        .collect::<Result<Vec<_>>>()
}

/// Returns the coerced exprs for each `input_exprs`.
/// Get the coerced data type from `aggregate_rule::coerce_types` and add `try_cast` if the
/// data type of `input_exprs` need to be coerced.
fn coerce_agg_exprs_for_signature(
    agg_fun: &AggregateFunction,
    input_exprs: &[Expr],
    schema: &DFSchema,
    signature: &Signature,
) -> Result<Vec<Expr>> {
    if input_exprs.is_empty() {
        return Ok(vec![]);
    }
    let current_types = input_exprs
        .iter()
        .map(|e| e.get_type(schema))
        .collect::<Result<Vec<_>>>()?;

    let coerced_types =
        type_coercion::aggregates::coerce_types(agg_fun, &current_types, signature)?;

    input_exprs
        .iter()
        .enumerate()
        .map(|(i, expr)| expr.clone().cast_to(&coerced_types[i], schema))
        .collect::<Result<Vec<_>>>()
}
