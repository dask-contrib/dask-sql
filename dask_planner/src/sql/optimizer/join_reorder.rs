//! Join reordering based on the paper "Improving Join Reordering for Large Scale Distributed Computing"
//! https://ieeexplore.ieee.org/document/9378281

use datafusion_common::Result;
use datafusion_expr::{Expr, Join, LogicalPlan, TableScan};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};

#[derive(Default)]
struct JoinReorder {}

impl OptimizerRule for JoinReorder {
    fn name(&self) -> &str {
        "join_reorder"
    }

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &mut OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        if let LogicalPlan::Join(join) = plan {
            if has_dominant_fact(join) && is_simple_join(join) && obeys_shuffle_constraint(join) {
                let tree_type = tree_type(join);
                let (fact, dims) = extract_fact_dimensions(join);
                let n = dims.len();

                let mut unfiltered_dimensions = get_unfiltered_dimensions(&dims);
                unfiltered_dimensions.push(Table::dummy(usize::MAX));
                //TODO sort unfiltered list on user-provided order in original
                // join, if not already in that order?

                let mut filtered_dimensions = get_filtered_dimensions(&dims);
                filtered_dimensions.push(Table::dummy(usize::MIN));
                // sort on size of dimension tables
                filtered_dimensions.sort_by(|a, b| a.size.cmp(&b.size));

                // Merge both the list of dimensions by giving user order
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
                let mut i = 0;
                let mut j = 0;
                let mut result = vec![];
                for r in 0..n {
                    if filtered_dimensions[i].size >= unfiltered_dimensions[j].size {
                        i += 1;
                        result.push(filtered_dimensions[i].clone());
                    } else {
                        j += 1;
                        result.push(unfiltered_dimensions[i].clone());
                    }
                }
                return Ok(Some(build_join_tree(tree_type, &fact, &result)));
            }
        }
        Ok(None)
    }

    fn optimize(&self, _plan: &LogicalPlan, _config: &mut OptimizerConfig) -> Result<LogicalPlan> {
        unimplemented!()
    }
}

/// Represents a Fact or Dimension table, or a dummy table.
#[derive(Clone)]
struct Table {
    filter: Option<Expr>,
    table_scan: Option<TableScan>,
    size: usize,
}

impl Table {
    /// Create a dummy table with infinite size
    fn dummy(size: usize) -> Self {
        Self {
            filter: None,
            table_scan: None,
            size,
        }
    }
}

#[derive(Debug)]
enum TreeType {
    LeftDeep,
    RightDeep,
}

/// Fact Table Constraint: Only if dominant fact table can
/// be determined from multi-way join then we can apply the
/// approach
fn has_dominant_fact(join: &Join) -> bool {
    todo!()
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
fn is_simple_join(join: &Join) -> bool {
    todo!()
}

/// We reorder only if the number of shuffles do not increase
fn obeys_shuffle_constraint(join: &Join) -> bool {
    todo!()
}

fn tree_type(join: &Join) -> TreeType {
    todo!()
}

/// Extract the fact table from the join, along with a list of dimension tables that
/// join to the fact table
fn extract_fact_dimensions(join: &Join) -> (Table, Vec<Table>) {
    todo!()
}

fn get_unfiltered_dimensions(dims: &[Table]) -> Vec<Table> {
    // TODO also look at filters pushed down to the table scan
    dims.iter()
        .filter(|t| t.filter.is_none())
        .cloned()
        .collect()
}

fn get_filtered_dimensions(dims: &[Table]) -> Vec<Table> {
    // TODO also look at filters pushed down to the table scan
    dims.iter()
        .filter(|t| t.filter.is_some())
        .cloned()
        .collect()
}

fn build_join_tree(tree_type: TreeType, fact: &Table, dims: &[Table]) -> LogicalPlan {
    todo!()
}

#[cfg(test)]
mod test {

    fn test() {}
}
