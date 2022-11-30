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

//! Filter Push Down optimizer rule ensures that filters are applied as early as possible in the plan

use std::{
    collections::{HashMap, HashSet},
    iter::once,
};

use datafusion_common::{Column, DFSchema, DataFusionError, Result};
use datafusion_expr::{
    col,
    expr_rewriter::{replace_col, ExprRewritable, ExprRewriter},
    logical_plan::{
        Aggregate,
        CrossJoin,
        Join,
        JoinType,
        Limit,
        LogicalPlan,
        Projection,
        TableScan,
        Union,
    },
    utils::{expr_to_columns, exprlist_to_columns, from_plan},
    Expr,
    TableProviderFilterPushDown,
};
use datafusion_optimizer::{utils, OptimizerConfig, OptimizerRule};

/// Filter Push Down optimizer rule pushes filter clauses down the plan
/// # Introduction
/// A filter-commutative operation is an operation whose result of filter(op(data)) = op(filter(data)).
/// An example of a filter-commutative operation is a projection; a counter-example is `limit`.
///
/// The filter-commutative property is column-specific. An aggregate grouped by A on SUM(B)
/// can commute with a filter that depends on A only, but does not commute with a filter that depends
/// on SUM(B).
///
/// This optimizer commutes filters with filter-commutative operations to push the filters
/// the closest possible to the scans, re-writing the filter expressions by every
/// projection that changes the filter's expression.
///
/// Filter: b Gt Int64(10)
///     Projection: a AS b
///
/// is optimized to
///
/// Projection: a AS b
///     Filter: a Gt Int64(10)  <--- changed from b to a
///
/// This performs a single pass through the plan. When it passes through a filter, it stores that filter,
/// and when it reaches a node that does not commute with it, it adds the filter to that place.
/// When it passes through a projection, it re-writes the filter's expression taking into account that projection.
/// When multiple filters would have been written, it `AND` their expressions into a single expression.
#[derive(Default)]
pub struct FilterPushDown {}

/// Filter predicate represented by tuple of expression and its columns
type Predicate = (Expr, HashSet<Column>);

/// Multiple filter predicates represented by tuple of expressions vector
/// and corresponding expression columns vector
type Predicates<'a> = (Vec<&'a Expr>, Vec<&'a HashSet<Column>>);

#[derive(Debug, Clone, Default)]
struct State {
    // (predicate, columns on the predicate)
    filters: Vec<Predicate>,
}

impl State {
    fn append_predicates(&mut self, predicates: Predicates) {
        predicates
            .0
            .into_iter()
            .zip(predicates.1)
            .for_each(|(expr, cols)| self.filters.push((expr.clone(), cols.clone())))
    }
}

/// returns all predicates in `state` that depend on any of `used_columns`
/// or the ones that does not reference any columns (e.g. WHERE 1=1)
fn get_predicates<'a>(state: &'a State, used_columns: &HashSet<Column>) -> Predicates<'a> {
    state
        .filters
        .iter()
        .filter(|(_, columns)| {
            columns.is_empty()
                || !columns
                    .intersection(used_columns)
                    .collect::<HashSet<_>>()
                    .is_empty()
        })
        .map(|&(ref a, ref b)| (a, b))
        .unzip()
}

/// Optimizes the plan
fn push_down(state: &State, plan: &LogicalPlan) -> Result<LogicalPlan> {
    let new_inputs = plan
        .inputs()
        .iter()
        .map(|input| optimize(input, state.clone()))
        .collect::<Result<Vec<_>>>()?;

    let expr = plan.expressions();
    from_plan(plan, &expr, &new_inputs)
}

// remove all filters from `filters` that are in `predicate_columns`
fn remove_filters(filters: &[Predicate], predicate_columns: &[&HashSet<Column>]) -> Vec<Predicate> {
    filters
        .iter()
        .filter(|(_, columns)| !predicate_columns.contains(&columns))
        .cloned()
        .collect::<Vec<_>>()
}

/// builds a new [LogicalPlan] from `plan` by issuing new [LogicalPlan::Filter] if any of the filters
/// in `state` depend on the columns `used_columns`.
fn issue_filters(
    mut state: State,
    used_columns: HashSet<Column>,
    plan: &LogicalPlan,
) -> Result<LogicalPlan> {
    let (predicates, predicate_columns) = get_predicates(&state, &used_columns);

    if predicates.is_empty() {
        // all filters can be pushed down => optimize inputs and return new plan
        return push_down(&state, plan);
    }

    let plan = utils::add_filter(plan.clone(), &predicates)?;

    state.filters = remove_filters(&state.filters, &predicate_columns);

    // continue optimization over all input nodes by cloning the current state (i.e. each node is independent)
    push_down(&state, &plan)
}

// For a given JOIN logical plan, determine whether each side of the join is preserved.
// We say a join side is preserved if the join returns all or a subset of the rows from
// the relevant side, such that each row of the output table directly maps to a row of
// the preserved input table. If a table is not preserved, it can provide extra null rows.
// That is, there may be rows in the output table that don't directly map to a row in the
// input table.
//
// For example:
//   - In an inner join, both sides are preserved, because each row of the output
//     maps directly to a row from each side.
//   - In a left join, the left side is preserved and the right is not, because
//     there may be rows in the output that don't directly map to a row in the
//     right input (due to nulls filling where there is no match on the right).
//
// This is important because we can always push down post-join filters to a preserved
// side of the join, assuming the filter only references columns from that side. For the
// non-preserved side it can be more tricky.
//
// Returns a tuple of booleans - (left_preserved, right_preserved).
fn lr_is_preserved(plan: &LogicalPlan) -> Result<(bool, bool)> {
    match plan {
        LogicalPlan::Join(Join { join_type, .. }) => match join_type {
            JoinType::Inner => Ok((true, true)),
            JoinType::Left => Ok((true, false)),
            JoinType::Right => Ok((false, true)),
            JoinType::Full => Ok((false, false)),
            // No columns from the right side of the join can be referenced in output
            // predicates for semi/anti joins, so whether we specify t/f doesn't matter.
            JoinType::LeftSemi | JoinType::LeftAnti => Ok((true, false)),
            _ => todo!(),
        },
        LogicalPlan::CrossJoin(_) => Ok((true, true)),
        _ => Err(DataFusionError::Internal(
            "lr_is_preserved only valid for JOIN nodes".to_string(),
        )),
    }
}

// For a given JOIN logical plan, determine whether each side of the join is preserved
// in terms on join filtering.
// Predicates from join filter can only be pushed to preserved join side.
fn on_lr_is_preserved(plan: &LogicalPlan) -> Result<(bool, bool)> {
    match plan {
        LogicalPlan::Join(Join { join_type, .. }) => match join_type {
            JoinType::Inner => Ok((true, true)),
            JoinType::Left => Ok((false, true)),
            JoinType::Right => Ok((true, false)),
            JoinType::Full => Ok((false, false)),
            JoinType::LeftSemi | JoinType::LeftAnti => {
                // filter_push_down does not yet support SEMI/ANTI joins with join conditions
                Ok((false, false))
            }
            _ => todo!(),
        },
        LogicalPlan::CrossJoin(_) => Err(DataFusionError::Internal(
            "on_lr_is_preserved cannot be applied to CROSSJOIN nodes".to_string(),
        )),
        _ => Err(DataFusionError::Internal(
            "on_lr_is_preserved only valid for JOIN nodes".to_string(),
        )),
    }
}

// Determine which predicates in state can be pushed down to a given side of a join.
// To determine this, we need to know the schema of the relevant join side and whether
// or not the side's rows are preserved when joining. If the side is not preserved, we
// do not push down anything. Otherwise we can push down predicates where all of the
// relevant columns are contained on the relevant join side's schema.
fn get_pushable_join_predicates<'a>(
    filters: &'a [Predicate],
    schema: &DFSchema,
    preserved: bool,
) -> Predicates<'a> {
    if !preserved {
        return (vec![], vec![]);
    }

    let schema_columns = schema
        .fields()
        .iter()
        .flat_map(|f| {
            [
                f.qualified_column(),
                // we need to push down filter using unqualified column as well
                f.unqualified_column(),
            ]
        })
        .collect::<HashSet<_>>();

    filters
        .iter()
        .filter(|(_, columns)| {
            let all_columns_in_schema = schema_columns
                .intersection(columns)
                .collect::<HashSet<_>>()
                .len()
                == columns.len();
            all_columns_in_schema
        })
        .map(|(a, b)| (a, b))
        .unzip()
}

fn optimize_join(
    mut state: State,
    plan: &LogicalPlan,
    left: &LogicalPlan,
    right: &LogicalPlan,
    on_filter: Vec<Predicate>,
) -> Result<LogicalPlan> {
    // Get pushable predicates from current optimizer state
    let (left_preserved, right_preserved) = lr_is_preserved(plan)?;
    let to_left = get_pushable_join_predicates(&state.filters, left.schema(), left_preserved);
    let to_right = get_pushable_join_predicates(&state.filters, right.schema(), right_preserved);
    let to_keep: Predicates = state
        .filters
        .iter()
        .filter(|(e, _)| !to_left.0.contains(&e) && !to_right.0.contains(&e))
        .map(|(a, b)| (a, b))
        .unzip();

    // Get pushable predicates from join filter
    let (on_to_left, on_to_right, on_to_keep) = if on_filter.is_empty() {
        ((vec![], vec![]), (vec![], vec![]), vec![])
    } else {
        let (on_left_preserved, on_right_preserved) = on_lr_is_preserved(plan)?;
        let on_to_left = get_pushable_join_predicates(&on_filter, left.schema(), on_left_preserved);
        let on_to_right =
            get_pushable_join_predicates(&on_filter, right.schema(), on_right_preserved);
        let on_to_keep = on_filter
            .iter()
            .filter(|(e, _)| !on_to_left.0.contains(&e) && !on_to_right.0.contains(&e))
            .map(|(a, _)| a.clone())
            .collect::<Vec<_>>();

        (on_to_left, on_to_right, on_to_keep)
    };

    // Build new filter states using pushable predicates
    // from current optimizer states and from ON clause.
    // Then recursively call optimization for both join inputs
    let mut left_state = State { filters: vec![] };
    left_state.append_predicates(to_left);
    left_state.append_predicates(on_to_left);
    let left = optimize(left, left_state)?;

    let mut right_state = State { filters: vec![] };
    right_state.append_predicates(to_right);
    right_state.append_predicates(on_to_right);
    let right = optimize(right, right_state)?;

    // Create a new Join with the new `left` and `right`
    //
    // expressions() output for Join is a vector consisting of
    //   1. join keys - columns mentioned in ON clause
    //   2. optional predicate - in case join filter is not empty,
    //      it always will be the last element, otherwise result
    //      vector will contain only join keys (without additional
    //      element representing filter).
    let expr = plan.expressions();
    let expr = if !on_filter.is_empty() && on_to_keep.is_empty() {
        // New filter expression is None - should remove last element
        expr[..expr.len() - 1].to_vec()
    } else if !on_to_keep.is_empty() {
        // Replace last element with new filter expression
        expr[..expr.len() - 1]
            .iter()
            .cloned()
            .chain(once(on_to_keep.into_iter().reduce(Expr::and).unwrap()))
            .collect()
    } else {
        plan.expressions()
    };
    let plan = from_plan(plan, &expr, &[left, right])?;

    if to_keep.0.is_empty() {
        Ok(plan)
    } else {
        // wrap the join on the filter whose predicates must be kept
        let plan = utils::add_filter(plan, &to_keep.0);
        state.filters = remove_filters(&state.filters, &to_keep.1);
        plan
    }
}

fn optimize(plan: &LogicalPlan, mut state: State) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Explain { .. } => {
            // push the optimization to the plan of this explain
            push_down(&state, plan)
        }
        LogicalPlan::Analyze { .. } => push_down(&state, plan),
        LogicalPlan::Filter(filter) => {
            let predicates = utils::split_conjunction(filter.predicate());

            predicates
                .into_iter()
                .try_for_each::<_, Result<()>>(|predicate| {
                    let mut columns: HashSet<Column> = HashSet::new();
                    expr_to_columns(predicate, &mut columns)?;
                    state.filters.push((predicate.clone(), columns));
                    Ok(())
                })?;

            optimize(filter.input(), state)
        }
        LogicalPlan::Projection(Projection {
            input,
            expr,
            schema,
            alias: _,
        }) => {
            // A projection is filter-commutable, but re-writes all predicate expressions
            // collect projection.
            let projection = schema
                .fields()
                .iter()
                .enumerate()
                .flat_map(|(i, field)| {
                    // strip alias, as they should not be part of filters
                    let expr = match &expr[i] {
                        Expr::Alias(expr, _) => expr.as_ref().clone(),
                        expr => expr.clone(),
                    };

                    // Convert both qualified and unqualified fields
                    [
                        (field.name().clone(), expr.clone()),
                        (field.qualified_name(), expr),
                    ]
                })
                .collect::<HashMap<_, _>>();

            // re-write all filters based on this projection
            // E.g. in `Filter: b\n  Projection: a > 1 as b`, we can swap them, but the filter must be "a > 1"
            for (predicate, columns) in state.filters.iter_mut() {
                *predicate = replace_cols_by_name(predicate.clone(), &projection)?;

                columns.clear();
                expr_to_columns(predicate, columns)?;
            }

            // optimize inner
            let new_input = optimize(input, state)?;
            Ok(from_plan(plan, expr, &[new_input])?)
        }
        LogicalPlan::Aggregate(Aggregate { aggr_expr, .. }) => {
            // An aggregate's aggreagate columns are _not_ filter-commutable => collect these:
            // * columns whose aggregation expression depends on
            // * the aggregation columns themselves

            // construct set of columns that `aggr_expr` depends on
            let mut used_columns = HashSet::new();
            exprlist_to_columns(aggr_expr, &mut used_columns)?;

            let agg_columns = aggr_expr
                .iter()
                .map(|x| Ok(Column::from_name(x.display_name()?)))
                .collect::<Result<HashSet<_>>>()?;
            used_columns.extend(agg_columns);

            issue_filters(state, used_columns, plan)
        }
        LogicalPlan::Sort { .. } => {
            // sort is filter-commutable
            push_down(&state, plan)
        }
        LogicalPlan::Union(Union {
            inputs: _,
            schema,
            alias: _,
        }) => {
            // union changing all qualifiers while building logical plan so we need
            // to rewrite filters to push unqualified columns to inputs
            let projection = schema
                .fields()
                .iter()
                .map(|field| (field.qualified_name(), col(field.name())))
                .collect::<HashMap<_, _>>();

            // rewriting predicate expressions using unqualified names as replacements
            if !projection.is_empty() {
                for (predicate, columns) in state.filters.iter_mut() {
                    *predicate = replace_cols_by_name(predicate.clone(), &projection)?;

                    columns.clear();
                    expr_to_columns(predicate, columns)?;
                }
            }

            push_down(&state, plan)
        }
        LogicalPlan::Limit(Limit { input, .. }) => {
            // limit is _not_ filter-commutable => collect all columns from its input
            let used_columns = input
                .schema()
                .fields()
                .iter()
                .map(|f| f.qualified_column())
                .collect::<HashSet<_>>();
            issue_filters(state, used_columns, plan)
        }
        LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => {
            optimize_join(state, plan, left, right, vec![])
        }
        LogicalPlan::Join(Join {
            left,
            right,
            on,
            filter,
            join_type,
            ..
        }) => {
            // Convert JOIN ON predicate to Predicates
            let on_filters = filter
                .as_ref()
                .map(|e| {
                    let predicates = utils::split_conjunction(e);

                    predicates
                        .into_iter()
                        .map(|e| {
                            let mut accum = HashSet::new();
                            expr_to_columns(e, &mut accum)?;
                            Ok((e.clone(), accum))
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .unwrap_or_else(|| Ok(vec![]))?;

            if *join_type == JoinType::Inner {
                // For inner joins, duplicate filters for joined columns so filters can be pushed down
                // to both sides. Take the following query as an example:
                //
                // ```sql
                // SELECT * FROM t1 JOIN t2 on t1.id = t2.uid WHERE t1.id > 1
                // ```
                //
                // `t1.id > 1` predicate needs to be pushed down to t1 table scan, while
                // `t2.uid > 1` predicate needs to be pushed down to t2 table scan.
                //
                // Join clauses with `Using` constraints also take advantage of this logic to make sure
                // predicates reference the shared join columns are pushed to both sides.
                // This logic should also been applied to conditions in JOIN ON clause
                let join_side_filters = state
                    .filters
                    .iter()
                    .chain(on_filters.iter())
                    .filter_map(|(predicate, columns)| {
                        let mut join_cols_to_replace = HashMap::new();
                        for col in columns.iter() {
                            for (l, r) in on {
                                if col == l {
                                    join_cols_to_replace.insert(col, r);
                                    break;
                                } else if col == r {
                                    join_cols_to_replace.insert(col, l);
                                    break;
                                }
                            }
                        }

                        if join_cols_to_replace.is_empty() {
                            return None;
                        }

                        let join_side_predicate =
                            match replace_col(predicate.clone(), &join_cols_to_replace) {
                                Ok(p) => p,
                                Err(e) => {
                                    return Some(Err(e));
                                }
                            };

                        let join_side_columns = columns
                            .clone()
                            .into_iter()
                            // replace keys in join_cols_to_replace with values in resulting column
                            // set
                            .filter(|c| !join_cols_to_replace.contains_key(c))
                            .chain(join_cols_to_replace.iter().map(|(_, v)| (*v).clone()))
                            .collect();

                        Some(Ok((join_side_predicate, join_side_columns)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                state.filters.extend(join_side_filters);
            }

            optimize_join(state, plan, left, right, on_filters)
        }
        LogicalPlan::TableScan(TableScan {
            source,
            projected_schema,
            filters,
            projection,
            table_name,
            fetch,
        }) => {
            let mut used_columns = HashSet::new();
            let mut new_filters = filters.clone();

            for (filter_expr, cols) in &state.filters {
                let (preserve_filter_node, add_to_provider) =
                    match source.supports_filter_pushdown(filter_expr)? {
                        TableProviderFilterPushDown::Unsupported => (true, false),
                        TableProviderFilterPushDown::Inexact => (true, true),
                        TableProviderFilterPushDown::Exact => (false, true),
                    };

                if preserve_filter_node {
                    used_columns.extend(cols.clone());
                }

                if add_to_provider {
                    // Don't add expression again if it's already present in
                    // pushed down filters.
                    if new_filters.contains(filter_expr) {
                        continue;
                    }
                    new_filters.push(filter_expr.clone());
                }
            }

            issue_filters(
                state,
                used_columns,
                &LogicalPlan::TableScan(TableScan {
                    source: source.clone(),
                    projection: projection.clone(),
                    projected_schema: projected_schema.clone(),
                    table_name: table_name.clone(),
                    filters: new_filters,
                    fetch: *fetch,
                }),
            )
        }
        _ => {
            // all other plans are _not_ filter-commutable
            let used_columns = plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.qualified_column())
                .collect::<HashSet<_>>();
            issue_filters(state, used_columns, plan)
        }
    }
}

impl OptimizerRule for FilterPushDown {
    fn name(&self) -> &str {
        "filter_push_down"
    }

    fn optimize(&self, plan: &LogicalPlan, _: &mut OptimizerConfig) -> Result<LogicalPlan> {
        optimize(plan, State::default())
    }
}

impl FilterPushDown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// replaces columns by its name on the projection.
fn replace_cols_by_name(e: Expr, replace_map: &HashMap<String, Expr>) -> Result<Expr> {
    struct ColumnReplacer<'a> {
        replace_map: &'a HashMap<String, Expr>,
    }

    impl<'a> ExprRewriter for ColumnReplacer<'a> {
        fn mutate(&mut self, expr: Expr) -> Result<Expr> {
            if let Expr::Column(c) = &expr {
                match self.replace_map.get(&c.flat_name()) {
                    Some(new_c) => Ok(new_c.clone()),
                    None => Ok(expr),
                }
            } else {
                Ok(expr)
            }
        }
    }

    e.rewrite(&mut ColumnReplacer { replace_map })
}
