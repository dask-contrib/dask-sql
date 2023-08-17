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
    datafusion_common::{Column, DataFusionError, Result},
    datafusion_expr::{
        expr::Exists,
        logical_plan::{Distinct, Filter, JoinType, Subquery},
        Expr,
        LogicalPlan,
        LogicalPlanBuilder,
    },
    datafusion_optimizer::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule},
};

use crate::sql::optimizer::utils::{
    collect_subquery_cols,
    conjunction,
    extract_join_filters,
    split_conjunction,
};

/// Optimizer rule for rewriting subquery filters to joins
#[derive(Default)]
pub struct DecorrelateWhereExists {}

impl DecorrelateWhereExists {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }

    /// Finds expressions that have a where in subquery (and recurse when found)
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
        let filters = split_conjunction(predicate);

        let mut subqueries = vec![];
        let mut others = vec![];
        for it in filters.iter() {
            match it {
                Expr::Exists(Exists { subquery, negated }) => {
                    let subquery_plan = self
                        .try_optimize(&subquery.subquery, config)?
                        .map(Arc::new)
                        .unwrap_or_else(|| subquery.subquery.clone());
                    let new_subquery = subquery.with_plan(subquery_plan);
                    subqueries.push(SubqueryInfo::new(new_subquery, *negated));
                }
                _ => others.push((*it).clone()),
            }
        }

        Ok((subqueries, others))
    }
}

impl OptimizerRule for DecorrelateWhereExists {
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
                    if let Some(x) = optimize_exists(&subquery, &cur_input)? {
                        cur_input = x;
                    } else {
                        return Ok(None);
                    }
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
        "decorrelate_where_exists"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

/// Takes a query like:
///
/// SELECT t1.id
/// FROM t1
/// WHERE exists
/// (
///    SELECT t2.id FROM t2 WHERE t1.id = t2.id
/// )
///
/// and optimizes it into:
///
/// SELECT t1.id
/// FROM t1 LEFT SEMI
/// JOIN t2
/// ON t1.id = t2.id
///
/// # Arguments
///
/// * query_info - The subquery and negated(exists/not exists) info.
/// * outer_input - The non-subquery portion (relation t1)
fn optimize_exists(
    query_info: &SubqueryInfo,
    outer_input: &LogicalPlan,
) -> Result<Option<LogicalPlan>> {
    let subquery = query_info.query.subquery.as_ref();
    if let Some((join_filter, optimized_subquery)) = optimize_subquery(subquery)? {
        // join our sub query into the main plan
        let join_type = match query_info.negated {
            true => JoinType::LeftAnti,
            false => JoinType::LeftSemi,
        };

        let new_plan = LogicalPlanBuilder::from(outer_input.clone())
            .join(
                optimized_subquery,
                join_type,
                (Vec::<Column>::new(), Vec::<Column>::new()),
                Some(join_filter),
            )?
            .build()?;

        Ok(Some(new_plan))
    } else {
        Ok(None)
    }
}
/// Optimize the subquery and extract the possible join filter.
/// This function can't optimize non-correlated subquery, and will return None.
fn optimize_subquery(subquery: &LogicalPlan) -> Result<Option<(Expr, LogicalPlan)>> {
    match subquery {
        LogicalPlan::Distinct(subqry_distinct) => {
            let distinct_input = &subqry_distinct.input;
            let optimized_plan = optimize_subquery(distinct_input)?.map(|(filters, right)| {
                (
                    filters,
                    LogicalPlan::Distinct(Distinct {
                        input: Arc::new(right),
                    }),
                )
            });
            Ok(optimized_plan)
        }
        LogicalPlan::Projection(projection) => {
            // extract join filters
            let (join_filters, subquery_input) = extract_join_filters(&projection.input)?;
            // cannot optimize non-correlated subquery
            if join_filters.is_empty() {
                return Ok(None);
            }
            let input_schema = subquery_input.schema();
            let project_exprs: Vec<Expr> =
                collect_subquery_cols(&join_filters, input_schema.clone())?
                    .into_iter()
                    .map(Expr::Column)
                    .collect();
            let right = LogicalPlanBuilder::from(subquery_input)
                .project(project_exprs)?
                .build()?;

            // join_filters is not empty.
            let join_filter = conjunction(join_filters).ok_or_else(|| {
                DataFusionError::Internal("join filters should not be empty".to_string())
            })?;
            Ok(Some((join_filter, right)))
        }
        _ => Ok(None),
    }
}

struct SubqueryInfo {
    query: Subquery,
    negated: bool,
}

impl SubqueryInfo {
    pub fn new(query: Subquery, negated: bool) -> Self {
        Self { query, negated }
    }
}
