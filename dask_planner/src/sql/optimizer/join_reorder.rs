//! Join reordering based on the paper "Improving Join Reordering for Large Scale Distributed Computing"
//! https://ieeexplore.ieee.org/document/9378281

use datafusion_common::Result;
use datafusion_expr::{Expr, Join, JoinType, LogicalPlan, TableScan};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};

pub struct JoinReorder {
    /// Ratio of the size of the largest Dimension table to Fact table
    fact_dimension_ratio: f64,
}

impl Default for JoinReorder {
    fn default() -> Self {
        Self {
            fact_dimension_ratio: 0.3,
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
        if let LogicalPlan::Join(join) = plan {
            // we can only reorder simple joins, as defined in `is_simple_join`
            if !is_simple_join(join) {
                println!("Not a simple join");
                return Ok(None);
            }

            let joins = extract_joins(join);
            let (fact, dims) = extract_fact_dimensions(&joins);
            if fact.is_none() {
                println!("There is no dominant fact table");
                return Ok(None);
            }
            let fact = fact.unwrap(); // unwrap here is safe due to previous check

            let tree_type = tree_type(join);
            let n = dims.len();

            let mut unfiltered_dimensions = get_unfiltered_dimensions(&dims);
            unfiltered_dimensions.push(Table::dummy(usize::MAX));
            //TODO sort unfiltered list on user-provided order in original
            // join, if not already in that order?

            let mut filtered_dimensions = get_filtered_dimensions(&dims);
            filtered_dimensions.push(Table::dummy(usize::MIN));
            // sort on size of dimension tables
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
            let mut i = 0;
            let mut j = 0;
            let mut result = vec![];
            for _ in 0..n {
                if filtered_dimensions[i].size <= unfiltered_dimensions[j].size {
                    i += 1;
                    result.push(filtered_dimensions[i].clone());
                } else {
                    j += 1;
                    result.push(unfiltered_dimensions[i].clone());
                }
            }
            return Ok(Some(build_join_tree(tree_type, &fact, &result)));
        }

        Ok(None)
    }

    fn optimize(&self, _plan: &LogicalPlan, _config: &mut OptimizerConfig) -> Result<LogicalPlan> {
        // this method is not needed because we implement try_optimize instead
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

/// build a list of all joins
fn extract_joins(join: &Join) -> Vec<Join> {
    fn extract_joins_inner(plan: &LogicalPlan, joins: &mut Vec<Join>) {
        match plan {
            LogicalPlan::Join(j) => {
                extract_joins_inner(&j.left, joins);
                extract_joins_inner(&j.right, joins);
                joins.push(j.clone())
            }
            other => {
                for child in other.inputs() {
                    extract_joins_inner(child, joins);
                }
            }
        }
    }

    let mut joins = vec![];
    extract_joins_inner(&LogicalPlan::Join(join.clone()), &mut joins);
    joins
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
    //TODO check for deterministic join/filter expressions

    fn is_simple_rel(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Filter(filter) => is_simple_rel(filter.input()),
            LogicalPlan::TableScan(_) => true,
            _ => false,
        }
    }

    join.join_type == JoinType::Inner && is_simple_rel(&join.left) && is_simple_rel(&join.right)
}

fn tree_type(_join: &Join) -> TreeType {
    //TODO implement
    TreeType::LeftDeep
}

/// Extract the fact table from the join, along with a list of dimension tables that
/// join to the fact table
fn extract_fact_dimensions(joins: &[Join]) -> (Option<Table>, Vec<Table>) {
    // at least half of joins should be inner joins involving one common table (the fact table)
    // other tables being joined with fact table are considered dimension tables

    for join in joins {
        if join.join_type == JoinType::Inner {}
    }

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

fn build_join_tree(_tree_type: TreeType, _fact: &Table, _dims: &[Table]) -> LogicalPlan {
    todo!()
}

#[cfg(test)]
mod tests {
    use std::{any::Any, sync::Arc};

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::Result;
    use datafusion_expr::{
        logical_plan::builder::LogicalTableSource,
        JoinType,
        LogicalPlan,
        LogicalPlanBuilder,
        TableProviderFilterPushDown,
        TableSource,
        TableType,
    };

    use super::*;

    #[test]
    fn inner_join_simple() -> Result<()> {
        let a = test_table_scan("t1", 100);
        let b = test_table_scan("t2", 100);
        let join = LogicalPlanBuilder::from(a)
            .join(&b, JoinType::Inner, (vec!["t1_a"], vec!["t2_b"]), None)?
            .build()?;
        if let LogicalPlan::Join(join) = join {
            assert!(is_simple_join(&join));
        } else {
            panic!();
        }
        Ok(())
    }

    #[test]
    fn outer_join_not_simple() -> Result<()> {
        let a = test_table_scan("t1", 100);
        let b = test_table_scan("t2", 100);
        let join = LogicalPlanBuilder::from(a)
            .join(&b, JoinType::Left, (vec!["t1_a"], vec!["t2_b"]), None)?
            .build()?;
        if let LogicalPlan::Join(join) = join {
            assert!(!is_simple_join(&join));
        } else {
            panic!();
        }
        Ok(())
    }

    #[test]
    fn test_extract_joins() -> Result<()> {
        let dim1 = test_table_scan("dim1", 100);
        let dim2 = test_table_scan("dim2", 200);
        let dim3 = test_table_scan("dim3", 300);
        let fact = test_table_scan("fact", 10000);
        let join = LogicalPlanBuilder::from(fact)
            .join(
                &dim1,
                JoinType::Inner,
                (vec!["fact_b"], vec!["dim1_a"]),
                None,
            )?
            .join(
                &dim2,
                JoinType::Inner,
                (vec!["fact_c"], vec!["dim2_a"]),
                None,
            )?
            .join(
                &dim3,
                JoinType::Inner,
                (vec!["fact_d"], vec!["dim3_a"]),
                None,
            )?
            .build()?;
        if let LogicalPlan::Join(join) = join {
            let joins = extract_joins(&join);
            assert_eq!(3, joins.len());
        } else {
            panic!()
        }
        Ok(())
    }

    fn test_table_scan(table_name: &str, size: usize) -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new(&format!("{}_a", table_name), DataType::UInt32, false),
            Field::new(&format!("{}_b", table_name), DataType::UInt32, false),
            Field::new(&format!("{}_c", table_name), DataType::UInt32, false),
            Field::new(&format!("{}_d", table_name), DataType::UInt32, false),
        ]);
        table_scan(Some(table_name), &schema, None)
            .expect("creating scan")
            .build()
            .expect("building plan")
    }

    fn table_scan(
        name: Option<&str>,
        table_schema: &Schema,
        projection: Option<Vec<usize>>,
    ) -> Result<LogicalPlanBuilder> {
        let tbl_schema = Arc::new(table_schema.clone());
        let table_source = Arc::new(LogicalTableSource::new(tbl_schema));
        LogicalPlanBuilder::scan(name.unwrap_or("test"), table_source, projection)
    }
}
