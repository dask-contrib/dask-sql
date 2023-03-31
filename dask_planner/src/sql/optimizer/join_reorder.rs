//! Join reordering based on the paper "Improving Join Reordering for Large Scale Distributed Computing"
//! https://ieeexplore.ieee.org/document/9378281

use std::collections::HashSet;

use datafusion_common::{Column, Result};
use datafusion_expr::{Expr, Join, JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion_optimizer::{utils, utils::split_conjunction, OptimizerConfig, OptimizerRule};
use log::warn;

use crate::sql::table::DaskTableSource;

pub struct JoinReorder {
    /// Maximum number of fact tables to allow in a join
    max_fact_tables: usize,
    /// Ratio of the size of the dimension tables to fact tables
    fact_dimension_ratio: f64,
    /// Whether to preserve user-defined order of unfiltered dimensions
    preserve_user_order: bool,
    /// Constant to use when determining the number of rows produced by a
    /// filtered relation
    filter_selectivity: f64,
}

impl Default for JoinReorder {
    fn default() -> Self {
        Self {
            max_fact_tables: 2,
            // FIXME: fact_dimension_ratio should be 0.3
            fact_dimension_ratio: 0.7,
            preserve_user_order: true,
            filter_selectivity: 1.0,
        }
    }
}

impl OptimizerRule for JoinReorder {
    fn name(&self) -> &str {
        "join_reorder"
    }

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        let original_plan = plan.clone();
        // Recurse down first
        // We want the equivalent of Spark's transformUp here
        let plan = utils::optimize_children(self, plan, _config)?;

        match &plan {
            Some(LogicalPlan::Join(join)) if join.join_type == JoinType::Inner => {
                optimize_join(self, plan.as_ref().unwrap(), join)
            }
            Some(plan) => Ok(Some(plan.clone())),
            None => match &original_plan {
                LogicalPlan::Join(join) if join.join_type == JoinType::Inner => {
                    optimize_join(self, &original_plan, join)
                }
                _ => Ok(None),
            },
        }
    }
}

fn optimize_join(
    rule: &JoinReorder,
    plan: &LogicalPlan,
    join: &Join,
) -> Result<Option<LogicalPlan>> {
    // FIXME: Check fact/fact join logic

    if !is_supported_join(join) {
        return Ok(Some(plan.clone()));
    }

    // Extract the relations and join conditions
    let (rels, conds) = extract_inner_joins(plan);

    let mut join_conds = HashSet::new();
    for cond in &conds {
        match cond {
            (Expr::Column(l), Expr::Column(r)) => {
                join_conds.insert((l.clone(), r.clone()));
            }
            _ => {
                return Ok(Some(plan.clone()));
            }
        }
    }

    // Split rels into facts and dims
    let largest_rel_size = rels.iter().map(|rel| rel.size).max().unwrap() as f64;
    // Vectors for the fact and dimension tables, respectively
    let mut facts = vec![];
    let mut dims = vec![];
    for rel in &rels {
        // If the ratio is larger than the fact_dimension_ratio, it is a fact table
        // Else, it is a dimension table
        if rel.size as f64 / largest_rel_size > rule.fact_dimension_ratio {
            facts.push(rel.clone());
        } else {
            dims.push(rel.clone());
        }
    }

    if facts.is_empty() || dims.is_empty() {
        return Ok(Some(plan.clone()));
    }
    if facts.len() > rule.max_fact_tables {
        return Ok(Some(plan.clone()));
    }

    // Get list of dimension tables without a selective predicate
    let mut unfiltered_dimensions = get_unfiltered_dimensions(&dims);
    if !rule.preserve_user_order {
        unfiltered_dimensions.sort_by(|a, b| a.size.cmp(&b.size));
    }

    // Get list of dimension tables with a selective predicate and sort it
    let filtered_dimensions = get_filtered_dimensions(&dims);
    let mut filtered_dimensions: Vec<Relation> = filtered_dimensions
        .iter()
        .map(|rel| Relation {
            plan: rel.plan.clone(),
            size: (rel.size as f64 * rule.filter_selectivity) as usize,
        })
        .collect();
    filtered_dimensions.sort_by(|a, b| a.size.cmp(&b.size));

    // Merge both the lists of dimensions by giving user order
    // the preference for tables without a selective predicate,
    // whereas for tables with selective predicates giving preference
    // to smaller tables. When comparing the top of both
    // the lists, if size of the top table in the selective predicate
    // list is smaller than top of the other list, choose it otherwise
    // vice-versa.
    // This algorithm is a greedy approach where smaller
    // joins with filtered dimension table are preferred for execution
    // earlier than other Joins to improve Join performance. We try to keep
    // the user order intact when unsure about reordering to make sure
    // regressions are minimized.
    let mut result = vec![];
    while !filtered_dimensions.is_empty() || !unfiltered_dimensions.is_empty() {
        if !filtered_dimensions.is_empty() {
            if !unfiltered_dimensions.is_empty() {
                if filtered_dimensions[0].size < unfiltered_dimensions[0].size {
                    result.push(filtered_dimensions.remove(0));
                } else {
                    result.push(unfiltered_dimensions.remove(0));
                }
            } else {
                result.push(filtered_dimensions.remove(0));
            }
        } else {
            result.push(unfiltered_dimensions.remove(0));
        }
    }

    let dim_plans: Vec<LogicalPlan> = result.iter().map(|rel| rel.plan.clone()).collect();

    let optimized = if facts.len() == 1 {
        build_join_tree(&facts[0].plan, &dim_plans, &mut join_conds)?
    } else {
        // Build one join tree for each fact table
        let fact_dim_joins = facts
            .iter()
            .map(|f| build_join_tree(&f.plan, &dim_plans, &mut join_conds))
            .collect::<Result<Vec<_>>>()?;
        // Join the trees together
        build_join_tree(&fact_dim_joins[0], &fact_dim_joins[1..], &mut join_conds)?
    };

    if join_conds.is_empty() {
        Ok(Some(optimized))
    } else {
        Ok(Some(plan.clone()))
    }
}

/// Represents a Fact or Dimension table, possibly nested in a filter
#[derive(Clone, Debug)]
struct Relation {
    /// Plan containing the table scan for the fact or dimension table
    /// May also contain Filter and SubqueryAlias
    plan: LogicalPlan,
    /// Estimated size of the underlying table before any filtering is applied
    size: usize,
}

impl Relation {
    fn new(plan: LogicalPlan) -> Self {
        let size = get_table_size(&plan);
        match size {
            Some(s) => Self { plan, size: s },
            None => {
                warn!("Table statistics couldn't be obtained; assuming 100 rows");
                Self { plan, size: 100 }
            }
        }
    }

    /// Determine if this plan contains any filters
    fn has_filter(&self) -> bool {
        has_filter(&self.plan)
    }
}

fn has_filter(plan: &LogicalPlan) -> bool {
    /// We want to ignore "IsNotNull" filters that are added for join keys since they exist
    /// for most dimension tables
    fn is_real_filter(predicate: &Expr) -> bool {
        let exprs = split_conjunction(predicate);
        let x = exprs
            .iter()
            .filter(|e| !matches!(e, Expr::IsNotNull(_)))
            .count();
        x > 0
    }

    match plan {
        LogicalPlan::Filter(filter) => is_real_filter(&filter.predicate),
        LogicalPlan::TableScan(scan) => scan.filters.iter().any(is_real_filter),
        _ => plan.inputs().iter().any(|child| has_filter(child)),
    }
}

/// Simple Join Constraint: Only INNER Joins are considered
/// which can be composed of other Joins too. But apart
/// from the Joins, none of the operator in both the left and
/// right side of the join should be non-deterministic, or have
/// output greater than the input to the operator. For instance,
/// Filter would be allowed operator as it reduces the output
/// over input, but a project adding extra column will not
/// be allowed. It is difficult to reason about operators that
/// add extra to output when dealing with just table sizes, so
/// instead we only allowed operators from selected set of
/// operators
fn is_supported_join(join: &Join) -> bool {
    // FIXME: Check for deterministic filter expressions

    fn is_supported_rel(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Join(join) => {
                join.join_type == JoinType::Inner
                    // FIXME: Need to support join filters correctly
                    && join.filter.is_none()
                    && is_supported_rel(&join.left)
                    && is_supported_rel(&join.right)
            }
            LogicalPlan::Filter(filter) => is_supported_rel(&filter.input),
            LogicalPlan::SubqueryAlias(sq) => is_supported_rel(&sq.input),
            LogicalPlan::TableScan(_) => true,
            _ => false,
        }
    }

    is_supported_rel(&LogicalPlan::Join(join.clone()))
}

/// Extracts items of consecutive inner joins and join conditions
/// This method works for bushy trees and left/right deep trees
fn extract_inner_joins(plan: &LogicalPlan) -> (Vec<Relation>, HashSet<(Expr, Expr)>) {
    fn _extract_inner_joins(
        plan: &LogicalPlan,
        rels: &mut Vec<LogicalPlan>,
        conds: &mut HashSet<(Expr, Expr)>,
    ) {
        match plan {
            LogicalPlan::Join(join)
                if join.join_type == JoinType::Inner && join.filter.is_none() =>
            {
                _extract_inner_joins(&join.left, rels, conds);
                _extract_inner_joins(&join.right, rels, conds);

                for (l, r) in &join.on {
                    conds.insert((l.clone(), r.clone()));
                }
            }
            /* FIXME: Need to support join filters correctly
            LogicalPlan::Join(join) if join.join_type == JoinType::Inner => {
                _extract_inner_joins(&join.left, rels, conds);
                _extract_inner_joins(&join.right, rels, conds);

                for (l, r) in &join.on {
                    conds.insert((l.clone(), r.clone()));
                }

                // Need to save this info somewhere
                let join_filter = join.filter.as_ref().unwrap();
            } */
            _ => {
                if find_join(plan).is_some() {
                    for x in plan.inputs() {
                        _extract_inner_joins(x, rels, conds);
                    }
                } else {
                    // Leaf node
                    rels.push(plan.clone())
                }
            }
        }
    }

    let mut rels = vec![];
    let mut conds = HashSet::new();
    _extract_inner_joins(plan, &mut rels, &mut conds);
    let rels = rels.into_iter().map(Relation::new).collect();
    (rels, conds)
}

/// Find first (top-level) join in plan
fn find_join(plan: &LogicalPlan) -> Option<Join> {
    match plan {
        LogicalPlan::Join(join) => Some(join.clone()),
        other => {
            if other.inputs().is_empty() {
                None
            } else {
                for input in &other.inputs() {
                    if let Some(join) = find_join(input) {
                        return Some(join);
                    }
                }
                None
            }
        }
    }
}

fn get_unfiltered_dimensions(dims: &[Relation]) -> Vec<Relation> {
    dims.iter().filter(|t| !t.has_filter()).cloned().collect()
}

fn get_filtered_dimensions(dims: &[Relation]) -> Vec<Relation> {
    dims.iter().filter(|t| t.has_filter()).cloned().collect()
}

fn build_join_tree(
    fact: &LogicalPlan,
    dims: &[LogicalPlan],
    conds: &mut HashSet<(Column, Column)>,
) -> Result<LogicalPlan> {
    let mut b = LogicalPlanBuilder::from(fact.clone());
    for dim in dims {
        // Find join keys between the fact and this dim
        let mut join_keys = vec![];
        for (l, r) in conds.iter() {
            if (b.schema().index_of_column(l).is_ok() && dim.schema().index_of_column(r).is_ok())
                || b.schema().index_of_column(r).is_ok() && dim.schema().index_of_column(l).is_ok()
            {
                join_keys.push((l.clone(), r.clone()));
            }
        }
        if !join_keys.is_empty() {
            let left_keys: Vec<Column> = join_keys.iter().map(|(l, _r)| l.clone()).collect();
            let right_keys: Vec<Column> = join_keys.iter().map(|(_l, r)| r.clone()).collect();

            for key in join_keys {
                conds.remove(&key);
            }

            /* FIXME: Build join with join_keys when needed
            self.join(
                right: LogicalPlan,
                join_type: JoinType,
                join_keys: (Vec<impl Into<Column>>, Vec<impl Into<Column>>),
                filter: Option<Expr>,
            ) */
            b = b.join(dim.clone(), JoinType::Inner, (left_keys, right_keys), None)?;
        }
    }
    b.build()
}

fn get_table_size(plan: &LogicalPlan) -> Option<usize> {
    match plan {
        LogicalPlan::TableScan(scan) => scan
            .source
            .as_any()
            .downcast_ref::<DaskTableSource>()
            .expect("should be a DaskTableSource")
            .statistics()
            .map(|stats| stats.get_row_count() as usize),
        _ => get_table_size(plan.inputs()[0]),
    }
}
