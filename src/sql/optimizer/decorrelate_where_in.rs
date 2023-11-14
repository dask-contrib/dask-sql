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

use std::sync::Arc;

use datafusion_python::{
    datafusion_common::{alias::AliasGenerator, context, Column, DataFusionError, Result},
    datafusion_expr::{
        expr::InSubquery,
        expr_rewriter::unnormalize_col,
        logical_plan::{JoinType, Projection, Subquery},
        Expr,
        Filter,
        LogicalPlan,
        LogicalPlanBuilder,
    },
    datafusion_optimizer::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule},
};
use log::debug;

use crate::sql::optimizer::utils::{
    collect_subquery_cols,
    conjunction,
    extract_join_filters,
    only_or_err,
    replace_qualified_name,
    split_conjunction,
};

#[derive(Default)]
pub struct DecorrelateWhereIn {
    alias: AliasGenerator,
}

impl DecorrelateWhereIn {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Finds expressions that have a where in subquery (and recurses when found)
    ///
    /// # Arguments
    ///
    /// * `predicate` - A conjunction to split and search
    /// * `optimizer_config` - For generating unique subquery aliases
    ///
    /// Returns a tuple (subqueries, non-subquery expressions)
    fn extract_subquery_exprs(
        &self,
        predicate: &Expr,
        config: &dyn OptimizerConfig,
    ) -> Result<(Vec<SubqueryInfo>, Vec<Expr>)> {
        let filters = split_conjunction(predicate); // TODO: disjunctions

        let mut subqueries = vec![];
        let mut others = vec![];
        for it in filters.iter() {
            match it {
                Expr::InSubquery(InSubquery {
                    expr,
                    subquery,
                    negated,
                }) => {
                    let subquery_plan = self
                        .try_optimize(&subquery.subquery, config)?
                        .map(Arc::new)
                        .unwrap_or_else(|| subquery.subquery.clone());
                    let new_subquery = subquery.with_plan(subquery_plan);
                    subqueries.push(SubqueryInfo::new(new_subquery, (**expr).clone(), *negated));
                    // TODO: if subquery doesn't get optimized, optimized children are lost
                }
                _ => others.push((*it).clone()),
            }
        }

        Ok((subqueries, others))
    }
}

impl OptimizerRule for DecorrelateWhereIn {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter(filter) => {
                let (subqueries, other_exprs) =
                    self.extract_subquery_exprs(&filter.predicate, config)?;
                if subqueries.is_empty() {
                    // regular filter, no subquery exists clause here
                    return Ok(None);
                }

                // iterate through all exists clauses in predicate, turning each into a join
                let mut cur_input = filter.input.as_ref().clone();
                for subquery in subqueries {
                    cur_input = optimize_where_in(&subquery, &cur_input, &self.alias)?;
                }

                let expr = conjunction(other_exprs);
                if let Some(expr) = expr {
                    let new_filter = Filter::try_new(expr, Arc::new(cur_input))?;
                    cur_input = LogicalPlan::Filter(new_filter);
                }

                Ok(Some(cur_input))
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "decorrelate_where_in"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

/// Optimize the where in subquery to left-anti/left-semi join.
/// If the subquery is a correlated subquery, we need extract the join predicate from the subquery.
///
/// For example, given a query like:
/// `select t1.a, t1.b from t1 where t1 in (select t2.a from t2 where t1.b = t2.b and t1.c > t2.c)`
///
/// The optimized plan will be:
///
/// ```text
/// Projection: t1.a, t1.b
///   LeftSemi Join:  Filter: t1.a = __correlated_sq_1.a AND t1.b = __correlated_sq_1.b AND t1.c > __correlated_sq_1.c
///     TableScan: t1
///     SubqueryAlias: __correlated_sq_1
///       Projection: t2.a AS a, t2.b, t2.c
///         TableScan: t2
/// ```
fn optimize_where_in(
    query_info: &SubqueryInfo,
    left: &LogicalPlan,
    alias: &AliasGenerator,
) -> Result<LogicalPlan> {
    let projection = try_from_plan(&query_info.query.subquery)
        .map_err(|e| context!("a projection is required", e))?;
    let subquery_input = projection.input.clone();
    // TODO add the validate logic to Analyzer
    let subquery_expr = only_or_err(projection.expr.as_slice())
        .map_err(|e| context!("single expression projection required", e))?;

    // extract join filters
    let (join_filters, subquery_input) = extract_join_filters(subquery_input.as_ref())?;

    // in_predicate may be also include in the join filters, remove it from the join filters.
    let in_predicate = Expr::eq(query_info.where_in_expr.clone(), subquery_expr.clone());
    let join_filters = remove_duplicated_filter(join_filters, in_predicate);

    // replace qualified name with subquery alias.
    let subquery_alias = alias.next("__correlated_sq");
    let input_schema = subquery_input.schema();
    let mut subquery_cols = collect_subquery_cols(&join_filters, input_schema.clone())?;
    let join_filter = conjunction(join_filters).map_or(Ok(None), |filter| {
        replace_qualified_name(filter, &subquery_cols, &subquery_alias).map(Option::Some)
    })?;

    // add projection
    if let Expr::Column(col) = subquery_expr {
        subquery_cols.remove(col);
    }
    let subquery_expr_name = format!("{:?}", unnormalize_col(subquery_expr.clone()));
    let first_expr = subquery_expr.clone().alias(subquery_expr_name.clone());
    let projection_exprs: Vec<Expr> = [first_expr]
        .into_iter()
        .chain(subquery_cols.into_iter().map(Expr::Column))
        .collect();

    let right = LogicalPlanBuilder::from(subquery_input)
        .project(projection_exprs)?
        .alias(subquery_alias.clone())?
        .build()?;

    // join our sub query into the main plan
    let join_type = match query_info.negated {
        true => JoinType::LeftAnti,
        false => JoinType::LeftSemi,
    };
    let right_join_col = Column::new(Some(subquery_alias), subquery_expr_name);
    let in_predicate = Expr::eq(
        query_info.where_in_expr.clone(),
        Expr::Column(right_join_col),
    );
    let join_filter = join_filter
        .map(|filter| in_predicate.clone().and(filter))
        .unwrap_or_else(|| in_predicate);

    let new_plan = LogicalPlanBuilder::from(left.clone())
        .join(
            right,
            join_type,
            (Vec::<Column>::new(), Vec::<Column>::new()),
            Some(join_filter),
        )?
        .build()?;

    debug!("where in optimized:\n{}", new_plan.display_indent());
    Ok(new_plan)
}

fn remove_duplicated_filter(filters: Vec<Expr>, in_predicate: Expr) -> Vec<Expr> {
    filters
        .into_iter()
        .filter(|filter| {
            if filter == &in_predicate {
                return false;
            }

            // ignore the binary order
            !match (filter, &in_predicate) {
                (Expr::BinaryExpr(a_expr), Expr::BinaryExpr(b_expr)) => {
                    (a_expr.op == b_expr.op)
                        && (a_expr.left == b_expr.left && a_expr.right == b_expr.right)
                        || (a_expr.left == b_expr.right && a_expr.right == b_expr.left)
                }
                _ => false,
            }
        })
        .collect::<Vec<_>>()
}

fn try_from_plan(plan: &LogicalPlan) -> Result<&Projection> {
    match plan {
        LogicalPlan::Projection(it) => Ok(it),
        _ => Err(DataFusionError::Internal(
            "Could not coerce into Projection!".to_string(),
        )),
    }
}

struct SubqueryInfo {
    query: Subquery,
    where_in_expr: Expr,
    negated: bool,
}

impl SubqueryInfo {
    pub fn new(query: Subquery, expr: Expr, negated: bool) -> Self {
        Self {
            query,
            where_in_expr: expr,
            negated,
        }
    }
}
