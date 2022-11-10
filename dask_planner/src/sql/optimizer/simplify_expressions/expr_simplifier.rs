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

//! Expression simplification API

use arrow::{
    array::new_null_array,
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    and,
    expr_rewriter::{ExprRewritable, ExprRewriter, RewriteRecursion},
    lit,
    or,
    BinaryExpr,
    BuiltinScalarFunction,
    ColumnarValue,
    Expr,
    Volatility,
};
use datafusion_optimizer::simplify_expressions::SimplifyInfo;
use datafusion_physical_expr::{create_physical_expr, execution_props::ExecutionProps};

use super::utils::*;
use crate::sql::optimizer::type_coercion::TypeCoercionRewriter;

/// This structure handles API for expression simplification
pub struct ExprSimplifier<S> {
    info: S,
}

// const THRESHOLD_INLINE_INLIST: usize = 3;

impl<S: SimplifyInfo> ExprSimplifier<S> {
    /// Create a new `ExprSimplifier` with the given `info` such as an
    /// instance of [`SimplifyContext`]. See
    /// [`simplify`](Self::simplify) for an example.
    pub fn new(info: S) -> Self {
        Self { info }
    }

    /// Simplifies this [`Expr`]`s as much as possible, evaluating
    /// constants and applying algebraic simplifications.
    ///
    /// The types of the expression must match what operators expect,
    /// or else an error may occur trying to evaluate. See
    /// [`coerce`](Self::coerce) for a function to help.
    ///
    /// # Example:
    ///
    /// `b > 2 AND b > 2`
    ///
    /// can be written to
    ///
    /// `b > 2`
    ///
    /// ```
    /// use datafusion_expr::{col, lit, Expr};
    /// use datafusion_common::Result;
    /// use datafusion_physical_expr::execution_props::ExecutionProps;
    /// use datafusion_optimizer::simplify_expressions::{ExprSimplifier, SimplifyInfo};
    ///
    /// /// Simple implementation that provides `Simplifier` the information it needs
    /// /// See SimplifyContext for a structure that does this.
    /// #[derive(Default)]
    /// struct Info {
    ///   execution_props: ExecutionProps,
    /// };
    ///
    /// impl SimplifyInfo for Info {
    ///   fn is_boolean_type(&self, expr: &Expr) -> Result<bool> {
    ///     Ok(false)
    ///   }
    ///   fn nullable(&self, expr: &Expr) -> Result<bool> {
    ///     Ok(true)
    ///   }
    ///   fn execution_props(&self) -> &ExecutionProps {
    ///     &self.execution_props
    ///   }
    /// }
    ///
    /// // Create the simplifier
    /// let simplifier = ExprSimplifier::new(Info::default());
    ///
    /// // b < 2
    /// let b_lt_2 = col("b").gt(lit(2));
    ///
    /// // (b < 2) OR (b < 2)
    /// let expr = b_lt_2.clone().or(b_lt_2.clone());
    ///
    /// // (b < 2) OR (b < 2) --> (b < 2)
    /// let expr = simplifier.simplify(expr).unwrap();
    /// assert_eq!(expr, b_lt_2);
    /// ```
    pub fn simplify(&self, expr: Expr) -> Result<Expr> {
        let mut simplifier = Simplifier::new(&self.info);
        let mut const_evaluator = ConstEvaluator::try_new(self.info.execution_props())?;

        // TODO iterate until no changes are made during rewrite
        // (evaluating constants can enable new simplifications and
        // simplifications can enable new constant evaluation)
        // https://github.com/apache/arrow-datafusion/issues/1160
        expr.rewrite(&mut const_evaluator)?
            .rewrite(&mut simplifier)?
            // run both passes twice to try an minimize simplifications that we missed
            .rewrite(&mut const_evaluator)?
            .rewrite(&mut simplifier)
    }
}

#[allow(rustdoc::private_intra_doc_links)]
/// Partially evaluate `Expr`s so constant subtrees are evaluated at plan time.
///
/// Note it does not handle algebraic rewrites such as `(a or false)`
/// --> `a`, which is handled by [`Simplifier`]
struct ConstEvaluator<'a> {
    /// can_evaluate is used during the depth-first-search of the
    /// Expr tree to track if any siblings (or their descendants) were
    /// non evaluatable (e.g. had a column reference or volatile
    /// function)
    ///
    /// Specifically, `can_evaluate[N]` represents the state of
    /// traversal when we are N levels deep in the tree, one entry for
    /// this Expr and each of its parents.
    ///
    /// After visiting all siblings if can_evauate.top() is true, that
    /// means there were no non evaluatable siblings (or their
    /// descendants) so this Expr can be evaluated
    can_evaluate: Vec<bool>,

    execution_props: &'a ExecutionProps,
    input_schema: DFSchema,
    input_batch: RecordBatch,
}

impl<'a> ExprRewriter for ConstEvaluator<'a> {
    fn pre_visit(&mut self, expr: &Expr) -> Result<RewriteRecursion> {
        // Default to being able to evaluate this node
        self.can_evaluate.push(true);

        // if this expr is not ok to evaluate, mark entire parent
        // stack as not ok (as all parents have at least one child or
        // descendant that can not be evaluated

        if !Self::can_evaluate(expr) {
            // walk back up stack, marking first parent that is not mutable
            let parent_iter = self.can_evaluate.iter_mut().rev();
            for p in parent_iter {
                if !*p {
                    // optimization: if we find an element on the
                    // stack already marked, know all elements above are also marked
                    break;
                }
                *p = false;
            }
        }

        // NB: do not short circuit recursion even if we find a non
        // evaluatable node (so we can fold other children, args to
        // functions, etc)
        Ok(RewriteRecursion::Continue)
    }

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        match self.can_evaluate.pop() {
            Some(true) => Ok(Expr::Literal(self.evaluate_to_scalar(expr)?)),
            Some(false) => Ok(expr),
            _ => Err(DataFusionError::Internal(
                "Failed to pop can_evaluate".to_string(),
            )),
        }
    }
}

impl<'a> ConstEvaluator<'a> {
    /// Create a new `ConstantEvaluator`. Session constants (such as
    /// the time for `now()` are taken from the passed
    /// `execution_props`.
    pub fn try_new(execution_props: &'a ExecutionProps) -> Result<Self> {
        // The dummy column name is unused and doesn't matter as only
        // expressions without column references can be evaluated
        static DUMMY_COL_NAME: &str = ".";
        let schema = Schema::new(vec![Field::new(DUMMY_COL_NAME, DataType::Null, true)]);
        let input_schema = DFSchema::try_from(schema.clone())?;
        // Need a single "input" row to produce a single output row
        let col = new_null_array(&DataType::Null, 1);
        let input_batch = RecordBatch::try_new(std::sync::Arc::new(schema), vec![col])?;

        Ok(Self {
            can_evaluate: vec![],
            execution_props,
            input_schema,
            input_batch,
        })
    }

    /// Can a function of the specified volatility be evaluated?
    fn volatility_ok(volatility: Volatility) -> bool {
        match volatility {
            Volatility::Immutable => true,
            // Values for functions such as now() are taken from ExecutionProps
            Volatility::Stable => true,
            Volatility::Volatile => false,
        }
    }

    /// Can the expression be evaluated at plan time, (assuming all of
    /// its children can also be evaluated)?
    fn can_evaluate(expr: &Expr) -> bool {
        // check for reasons we can't evaluate this node
        //
        // NOTE all expr types are listed here so when new ones are
        // added they can be checked for their ability to be evaluated
        // at plan time
        match expr {
            // Has no runtime cost, but needed during planning
            Expr::Alias(..)
            | Expr::AggregateFunction { .. }
            | Expr::AggregateUDF { .. }
            | Expr::ScalarVariable(_, _)
            | Expr::Column(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::ScalarSubquery(_)
            | Expr::WindowFunction { .. }
            | Expr::Sort { .. }
            | Expr::GroupingSet(_)
            | Expr::Wildcard
            | Expr::QualifiedWildcard { .. } => false,
            Expr::ScalarFunction { fun, .. } => Self::volatility_ok(fun.volatility()),
            Expr::ScalarUDF { fun, .. } => Self::volatility_ok(fun.signature.volatility),
            Expr::Literal(_)
            | Expr::BinaryExpr { .. }
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Negative(_)
            | Expr::Between { .. }
            | Expr::Like { .. }
            | Expr::ILike { .. }
            | Expr::SimilarTo { .. }
            | Expr::Case(_)
            | Expr::Cast { .. }
            | Expr::TryCast { .. }
            | Expr::InList { .. }
            | Expr::GetIndexedField { .. } => true,
        }
    }

    /// Internal helper to evaluates an Expr
    pub(crate) fn evaluate_to_scalar(&mut self, expr: Expr) -> Result<ScalarValue> {
        if let Expr::Literal(s) = expr {
            return Ok(s);
        }

        let phys_expr = create_physical_expr(
            &expr,
            &self.input_schema,
            &self.input_batch.schema(),
            self.execution_props,
        )?;
        let col_val = phys_expr.evaluate(&self.input_batch)?;
        match col_val {
            ColumnarValue::Array(a) => {
                if a.len() != 1 {
                    Err(DataFusionError::Execution(format!(
                        "Could not evaluate the expression, found a result of length {}",
                        a.len()
                    )))
                } else {
                    Ok(ScalarValue::try_from_array(&a, 0)?)
                }
            }
            ColumnarValue::Scalar(s) => Ok(s),
        }
    }
}

/// Simplifies [`Expr`]s by applying algebraic transformation rules
///
/// Example transformations that are applied:
/// * `expr = true` and `expr != false` to `expr` when `expr` is of boolean type
/// * `expr = false` and `expr != true` to `!expr` when `expr` is of boolean type
/// * `true = true` and `false = false` to `true`
/// * `false = true` and `true = false` to `false`
/// * `!!expr` to `expr`
/// * `expr = null` and `expr != null` to `null`
struct Simplifier<'a, S> {
    info: &'a S,
}

impl<'a, S> Simplifier<'a, S> {
    pub fn new(info: &'a S) -> Self {
        Self { info }
    }
}

impl<'a, S: SimplifyInfo> ExprRewriter for Simplifier<'a, S> {
    /// rewrite the expression simplifying any constant expressions
    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        use datafusion_expr::Operator::{And, Divide, Eq, Modulo, Multiply, NotEq, Or};

        let info = self.info;
        let new_expr = match expr {
            //
            // Rules for Eq
            //

            // true = A  --> A
            // false = A --> !A
            // null = A --> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Eq,
                right,
            }) if is_bool_lit(&left) && info.is_boolean_type(&right)? => {
                match as_bool_lit(*left)? {
                    Some(true) => *right,
                    Some(false) => Expr::Not(right),
                    None => lit_bool_null(),
                }
            }
            // A = true  --> A
            // A = false --> !A
            // A = null --> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Eq,
                right,
            }) if is_bool_lit(&right) && info.is_boolean_type(&left)? => {
                match as_bool_lit(*right)? {
                    Some(true) => *left,
                    Some(false) => Expr::Not(left),
                    None => lit_bool_null(),
                }
            }
            // expr IN () --> false
            // expr NOT IN () --> true
            Expr::InList {
                expr,
                list,
                negated,
            } if list.is_empty() && *expr != Expr::Literal(ScalarValue::Null) => lit(negated),

            // if expr is a single column reference:
            // expr IN (A, B, ...) --> (expr = A) OR (expr = B) OR (expr = C)
            // Expr::InList {
            //     expr,
            //     list,
            //     negated,
            // } if !list.is_empty()
            //     && (
            //         // For lists with only 1 value we allow more complex expressions to be simplified
            //         // e.g SUBSTR(c1, 2, 3) IN ('1') -> SUBSTR(c1, 2, 3) = '1'
            //         // for more than one we avoid repeating this potentially expensive
            //         // expressions
            //         list.len() == 1
            //             || list.len() <= THRESHOLD_INLINE_INLIST && expr.try_into_col().is_ok()
            //     ) =>
            // {
            //     let first_val = list[0].clone();
            //     if negated {
            //         list.into_iter()
            //             .skip(1)
            //             .fold((*expr.clone()).not_eq(first_val), |acc, y| {
            //                 (*expr.clone()).not_eq(y).and(acc)
            //             })
            //     } else {
            //         list.into_iter()
            //             .skip(1)
            //             .fold((*expr.clone()).eq(first_val), |acc, y| {
            //                 (*expr.clone()).eq(y).or(acc)
            //             })
            //     }
            // }
            //
            // Rules for NotEq
            //

            // true != A  --> !A
            // false != A --> A
            // null != A --> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: NotEq,
                right,
            }) if is_bool_lit(&left) && info.is_boolean_type(&right)? => {
                match as_bool_lit(*left)? {
                    Some(true) => Expr::Not(right),
                    Some(false) => *right,
                    None => lit_bool_null(),
                }
            }
            // A != true  --> !A
            // A != false --> A
            // A != null --> null,
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: NotEq,
                right,
            }) if is_bool_lit(&right) && info.is_boolean_type(&left)? => {
                match as_bool_lit(*right)? {
                    Some(true) => Expr::Not(left),
                    Some(false) => *left,
                    None => lit_bool_null(),
                }
            }

            //
            // Rules for OR
            //

            // true OR A --> true (even if A is null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right: _,
            }) if is_true(&left) => *left,
            // false OR A --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right,
            }) if is_false(&left) => *right,
            // A OR true --> true (even if A is null)
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op: Or,
                right,
            }) if is_true(&right) => *right,
            // A OR false --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right,
            }) if is_false(&right) => *left,
            // (..A..) OR A --> (..A..)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right,
            }) if expr_contains(&left, &right, Or) => *left,
            // A OR (..A..) --> (..A..)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right,
            }) if expr_contains(&right, &left, Or) => *right,
            // A OR (A AND B) --> A (if B not null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right,
            }) if !info.nullable(&right)? && is_op_with(And, &right, &left) => *left,
            // (A AND B) OR A --> A (if B not null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right,
            }) if !info.nullable(&left)? && is_op_with(And, &left, &right) => *right,

            //
            // Rules for AND
            //

            // true AND A --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right,
            }) if is_true(&left) => *right,
            // false AND A --> false (even if A is null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right: _,
            }) if is_false(&left) => *left,
            // A AND true --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right,
            }) if is_true(&right) => *left,
            // A AND false --> false (even if A is null)
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op: And,
                right,
            }) if is_false(&right) => *right,
            // (..A..) AND A --> (..A..)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right,
            }) if expr_contains(&left, &right, And) => *left,
            // A AND (..A..) --> (..A..)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right,
            }) if expr_contains(&right, &left, And) => *right,
            // A AND (A OR B) --> A (if B not null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right,
            }) if !info.nullable(&right)? && is_op_with(Or, &right, &left) => *left,
            // (A OR B) AND A --> A (if B not null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right,
            }) if !info.nullable(&left)? && is_op_with(Or, &left, &right) => *right,

            //
            // Rules for Multiply
            //

            // A * 1 --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Multiply,
                right,
            }) if is_one(&right) => *left,
            // 1 * A --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Multiply,
                right,
            }) if is_one(&left) => *right,
            // A * null --> null
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op: Multiply,
                right,
            }) if is_null(&right) => *right,
            // null * A --> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Multiply,
                right: _,
            }) if is_null(&left) => *left,

            // A * 0 --> 0 (if A is not null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Multiply,
                right,
            }) if !info.nullable(&left)? && is_zero(&right) => *right,
            // 0 * A --> 0 (if A is not null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Multiply,
                right,
            }) if !info.nullable(&right)? && is_zero(&left) => *left,

            //
            // Rules for Divide
            //

            // A / 1 --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Divide,
                right,
            }) if is_one(&right) => *left,
            // null / A --> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Divide,
                right: _,
            }) if is_null(&left) => *left,
            // A / null --> null
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op: Divide,
                right,
            }) if is_null(&right) => *right,
            // 0 / 0 -> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Divide,
                right,
            }) if is_zero(&left) && is_zero(&right) => Expr::Literal(ScalarValue::Int32(None)),
            // A / 0 -> DivideByZero Error
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Divide,
                right,
            }) if !info.nullable(&left)? && is_zero(&right) => {
                return Err(DataFusionError::ArrowError(ArrowError::DivideByZero));
            }

            //
            // Rules for Modulo
            //

            // A % null --> null
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op: Modulo,
                right,
            }) if is_null(&right) => *right,
            // null % A --> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Modulo,
                right: _,
            }) if is_null(&left) => *left,
            // A % 1 --> 0
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Modulo,
                right,
            }) if !info.nullable(&left)? && is_one(&right) => lit(0),
            // A % 0 --> DivideByZero Error
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Modulo,
                right,
            }) if !info.nullable(&left)? && is_zero(&right) => {
                return Err(DataFusionError::ArrowError(ArrowError::DivideByZero));
            }

            //
            // Rules for Not
            //
            Expr::Not(inner) => negate_clause(*inner),

            //
            // Rules for Case
            //

            // CASE
            //   WHEN X THEN A
            //   WHEN Y THEN B
            //   ...
            //   ELSE Q
            // END
            //
            // ---> (X AND A) OR (Y AND B AND NOT X) OR ... (NOT (X OR Y) AND Q)
            //
            // Note: the rationale for this rewrite is that the expr can then be further
            // simplified using the existing rules for AND/OR
            Expr::Case(case)
                if !case.when_then_expr.is_empty()
                && case.when_then_expr.len() < 3 // The rewrite is O(n!) so limit to small number
                && info.is_boolean_type(&case.when_then_expr[0].1)? =>
            {
                // The disjunction of all the when predicates encountered so far
                let mut filter_expr = lit(false);
                // The disjunction of all the cases
                let mut out_expr = lit(false);

                for (when, then) in case.when_then_expr {
                    let case_expr = when
                        .as_ref()
                        .clone()
                        .and(filter_expr.clone().not())
                        .and(*then);

                    out_expr = out_expr.or(case_expr);
                    filter_expr = filter_expr.or(*when);
                }

                if let Some(else_expr) = case.else_expr {
                    let case_expr = filter_expr.not().and(*else_expr);
                    out_expr = out_expr.or(case_expr);
                }

                // Do a first pass at simplification
                out_expr.rewrite(self)?
            }

            // concat
            Expr::ScalarFunction {
                fun: BuiltinScalarFunction::Concat,
                args,
            } => simpl_concat(args)?,

            // concat_ws
            Expr::ScalarFunction {
                fun: BuiltinScalarFunction::ConcatWithSeparator,
                args,
            } => match &args[..] {
                [delimiter, vals @ ..] => simpl_concat_ws(delimiter, vals)?,
                _ => Expr::ScalarFunction {
                    fun: BuiltinScalarFunction::ConcatWithSeparator,
                    args,
                },
            },

            //
            // Rules for Between
            //

            // a between 3 and 5  -->  a >= 3 AND a <=5
            // a not between 3 and 5  -->  a < 3 OR a > 5
            Expr::Between(between) => {
                if between.negated {
                    let l = *between.expr.clone();
                    let r = *between.expr;
                    or(l.lt(*between.low), r.gt(*between.high))
                } else {
                    and(
                        between.expr.clone().gt_eq(*between.low),
                        between.expr.lt_eq(*between.high),
                    )
                }
            }
            expr => {
                // no additional rewrites possible
                expr
            }
        };
        Ok(new_expr)
    }
}
