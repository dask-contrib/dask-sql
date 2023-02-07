//! Join reordering based on the paper "Improving Join Reordering for Large Scale Distributed Computing"
//! https://ieeexplore.ieee.org/document/9378281

use std::collections::HashSet;

use datafusion_common::{Column, Result};
use datafusion_expr::{Expr, Join, JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion_optimizer::{utils, utils::split_conjunction, OptimizerConfig, OptimizerRule};

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
            fact_dimension_ratio: 0.3,
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
        _config: &mut OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        // Recurse down first
        // We want the equivalent of Spark's transformUp here
        let plan = utils::optimize_children(self, plan, _config)?;

        println!("JoinReorder::try_optimize():\n{}", plan.display_indent());  // TODO: Remove
    
        match &plan {
            LogicalPlan::Join(join) if join.join_type == JoinType::Inner => {
                if !is_supported_join(join) {
                    println!("Not a supported join!:\n{}", plan.display_indent());  // TODO: Remove
                    return Ok(Some(plan));
                }
                println!(
                    "JoinReorder attempting to optimize join: {}",
                    plan.display_indent()
                );  // TODO: Remove

                // Extract the relations and join conditions
                let (rels, conds) = extract_inner_joins(&plan);

                // Split rels into facts and dims
                let rels: Vec<Relation> = rels.into_iter().map(|rel| Relation::new(rel)).collect();
                let largest_rel = rels.iter().map(|rel| rel.size).max().unwrap() as f64;
                let mut facts = vec![];
                let mut dims = vec![];
                for rel in &rels {
                    println!("rel size = {}", rel.size);  // TODO: Remove
                    if rel.size as f64 / largest_rel > self.fact_dimension_ratio {
                        facts.push(rel.clone());
                    } else {
                        dims.push(rel.clone());
                    }
                }
                println!("There are {} facts and {} dims", facts.len(), dims.len());  // TODO: Remove
                if facts.is_empty() {
                    println!("Too few fact tables");  // TODO: Remove
                    return Ok(Some(plan));
                }
                if facts.len() > self.max_fact_tables {
                    println!("Too many fact tables");  // TODO: Remove
                    return Ok(Some(plan));
                }

                let mut unfiltered_dimensions = get_unfiltered_dimensions(&dims);
                if !self.preserve_user_order {
                    unfiltered_dimensions.sort_by(|a, b| a.size.cmp(&b.size));
                }

                let filtered_dimensions = get_filtered_dimensions(&dims);
                let mut filtered_dimensions: Vec<Relation> = filtered_dimensions
                    .iter()
                    .map(|rel| Relation {
                        plan: rel.plan.clone(),
                        size: (rel.size as f64 * self.filter_selectivity) as usize,
                    })
                    .collect();

                filtered_dimensions.sort_by(|a, b| a.size.cmp(&b.size));
                for dim in &unfiltered_dimensions {
                    println!("UNFILTERED: {} {}", dim.size, dim.plan.display_indent());  // TODO: Remove
                }

                for dim in &filtered_dimensions {
                    println!("FILTERED: {} {}", dim.size, dim.plan.display_indent());  // TODO: Remove
                }

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
                    if !filtered_dimensions.is_empty() && !unfiltered_dimensions.is_empty() {
                        if filtered_dimensions[0].size < unfiltered_dimensions[0].size {
                            result.push(filtered_dimensions.remove(0));
                        } else {
                            result.push(unfiltered_dimensions.remove(0));
                        }
                    } else if !filtered_dimensions.is_empty() {
                        result.push(filtered_dimensions.remove(0));
                    } else {
                        result.push(unfiltered_dimensions.remove(0));
                    }
                }
                assert!(filtered_dimensions.is_empty());
                assert!(unfiltered_dimensions.is_empty());

                let dim_plans: Vec<LogicalPlan> =
                    result.iter().map(|rel| rel.plan.clone()).collect();
    
                let mut join_conds = HashSet::new();
                for cond in &conds {
                    match cond {
                        (Expr::Column(l), Expr::Column(r)) => {
                            join_conds.insert((l.clone(), r.clone()));
                        }
                        _ => {
                            println!("Only column expr are supported");  // TODO: Remove
                            return Ok(Some(plan));
                        }
                    }
                }

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
                    println!("Optimized: {}", optimized.display_indent());  // TODO: Remove
                    return Ok(Some(optimized));
                } else {
                    println!("Did not use all join conditions: {:?}", join_conds);  // TODO: Remove
                    return Ok(Some(plan));
                }
            }
            _ => {
                println!("not a join");  // TODO: Remove
                Ok(Some(plan))
            }
        }
    }

    fn optimize(&self, _plan: &LogicalPlan, _config: &mut OptimizerConfig) -> Result<LogicalPlan> {
        // This method is not needed because we implement try_optimize instead
        unimplemented!()
    }
}

/// Represents a Fact or Dimension table, possibly nested in a filter.
#[derive(Clone, Debug)]
struct Relation {
    /// Plan containing the table scan for the fact or dimension table. May also contain
    /// Filter and SubqueryAlias.
    plan: LogicalPlan,
    /// Estimated size of the underlying table before any filtering is applied
    size: usize,
}

impl Relation {
    fn new(plan: LogicalPlan) -> Self {
        let size = get_table_size(&plan).unwrap_or(100);
        Self { plan, size }
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
        LogicalPlan::Filter(filter) => is_real_filter(&filter.predicate()),
        LogicalPlan::TableScan(scan) => scan.filters.iter().any(is_real_filter),
        _ => plan.inputs().iter().any(|child| has_filter(child)),
    }
}

/// Extracts items of consecutive inner joins and join conditions.
/// This method works for bushy trees and left/right deep trees.
fn extract_inner_joins(plan: &LogicalPlan) -> (Vec<LogicalPlan>, HashSet<(Expr, Expr)>) {
    fn _extract_inner_joins(
        plan: &LogicalPlan,
        rels: &mut Vec<LogicalPlan>,
        conds: &mut HashSet<(Expr, Expr)>,
    ) {
        match plan {
            LogicalPlan::Join(join)
                if join.join_type == JoinType::Inner /* TODO && join.filter.is_none()*/ =>
            {
                _extract_inner_joins(&join.left, rels, conds);
                _extract_inner_joins(&join.right, rels, conds);
                // TODO: Could also handle join conditions here?

                for (l, r) in &join.on {
                    conds.insert((
                        datafusion_expr::Expr::Column(l.clone()),  // TODO: Expr vs Column
                        datafusion_expr::Expr::Column(r.clone()),  // TODO: Expr vs Column
                    ));
                }
            }
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
    (rels, conds)
}

/// Simple Join Constraint: Only INNER Joins are consid-
/// ered which can be composed of other Joins too. But apart
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
    // TODO: Check for deterministic filter expressions

    fn is_supported_rel(plan: &LogicalPlan) -> bool {
        println!("is_simple_rel? {}", plan.display_indent());  // TODO: Remove
        match plan {
            LogicalPlan::Join(join) => {
                join.join_type == JoinType::Inner
                    // TODO: Need to support join filters correctly .. for now assume
                    // they have already been pushed down to the underlying table scan
                    // but we need to make sure we do not drop these filters when
                    // rebuilding the joins later
                    // && join.filter.is_none()
                    && is_supported_rel(&join.left)
                    && is_supported_rel(&join.right)
            }
            LogicalPlan::Filter(filter) => is_supported_rel(&filter.input()),
            LogicalPlan::SubqueryAlias(sq) => is_supported_rel(&sq.input),
            LogicalPlan::TableScan(_) => true,
            _ => {
                println!("not a simple join: {}", plan.display_indent());  // TODO: Remove
                false
            }
        }
    }

    is_supported_rel(&LogicalPlan::Join(join.clone()))
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
                || b.schema().index_of_column(r).is_ok()
                    && dim.schema().index_of_column(l).is_ok()
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

            println!("Joining fact to dim on {:?} = {:?}", left_keys, right_keys);  // TODO: Remove
            b = b.join(&dim, JoinType::Inner, (left_keys, right_keys), None)?;
        }
    }
    b.build()
}

fn get_table_size(plan: &LogicalPlan) -> Option<usize> {
    // TODO
    /*match plan {
        LogicalPlan::TableScan(scan) => {
            if let Some(stats) = scan.source.statistics() {
                stats.num_rows
            } else {
                Some(100)
            }
        }
        _ => get_table_size(plan.inputs()[0]),
    }*/
    match plan {
        LogicalPlan::TableScan(scan) => {
            let source = scan
                .source
                .as_any()
                .downcast_ref::<DaskTableSource>()
                .expect("should be a DaskTableSource");
            if let Some(stats) = source.statistics() {
                stats.num_rows
            } else {
                // TODO: Hard-coded stats for manual testing until stats are available
                // these numbers based on sf100
                let n = match scan.table_name.as_str() {
                    "call_center" => 30,
                    "catalog_page" => 20400,
                    "catalog_returns" => 14404374,
                    "catalog_sales" => 143997065,
                    "customer_address" => 1000000,
                    "customer_demographics" => 1920800,
                    "customer" => 2000000,
                    "date_dim" => 73049,
                    "household_demographics" => 7200,
                    "income_band" => 20,
                    "inventory" => 399330000,
                    "item" => 204000,
                    "promotion" => 1000,
                    "reason" => 55,
                    "ship_mode" => 20,
                    "store" => 402,
                    "store_returns" => 28795080,
                    "store_sales" => 287997024,
                    "time_dim" => 86400,
                    "warehouse" => 15,
                    "web_page" => 2040,
                    "web_returns" => 7197670,
                    "web_sales" => 72001237,
                    "web_site" => 24,
                    other => {
                        println!("No row count available for table '{}'", other);  // TODO: Remove
                        100
                    }
                };

                Some(n)
            }
        }
        _ => get_table_size(&plan.inputs()[0]),
    }
}

// TODO: Add Rust tests
