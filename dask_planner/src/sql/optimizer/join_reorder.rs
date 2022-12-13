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
        //TODO is this transformUp or transformDown?
        // TODO too many clones - use Box/Rc/Arc to reduce
        match plan {
            LogicalPlan::Join(join) if join.join_type == JoinType::Inner => {
                if !is_supported_join(join) {
                    println!("Not a supported join");
                    return Ok(None);
                }
                println!(
                    "JoinReorder attempting to optimize join: {}",
                    plan.display_indent()
                );

                // extract the relations and join conditions
                let (rels, mut conds) = extract_inner_joins(plan);

                // split rels into facts and dims
                let rels: Vec<Relation> = rels.into_iter().map(|rel| Relation::new(rel)).collect();
                let largest_rel = rels.iter().map(|rel| rel.size).max().unwrap() as f64;
                let mut facts = vec![];
                let mut dims = vec![];
                for rel in &rels {
                    if rel.size as f64 / largest_rel > self.fact_dimension_ratio {
                        facts.push(rel.clone());
                    } else {
                        dims.push(rel.clone());
                    }
                }
                println!("There are {} facts and {} dims", facts.len(), dims.len());
                if facts.is_empty() {
                    return Ok(None);
                }

                let mut unfiltered_dimensions = get_unfiltered_dimensions(&dims);
                let mut filtered_dimensions = get_filtered_dimensions(&dims);
                filtered_dimensions.sort_by(|a, b| a.size.cmp(&b.size));
                for dim in &unfiltered_dimensions {
                    println!("UNFILTERED: {} {}", dim.size, dim.plan.display_indent());
                }

                for dim in &filtered_dimensions {
                    println!("FILTERED: {} {}", dim.size, dim.plan.display_indent());
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
                while filtered_dimensions.len() > 0 || unfiltered_dimensions.len() > 0 {
                    if filtered_dimensions.len() > 0 && unfiltered_dimensions.len() > 0 {
                        if filtered_dimensions[0].size < unfiltered_dimensions[0].size {
                            result.push(filtered_dimensions.remove(0));
                        } else {
                            result.push(unfiltered_dimensions.remove(0));
                        }
                    } else if filtered_dimensions.len() > 0 {
                        result.push(filtered_dimensions.remove(0));
                    } else {
                        result.push(unfiltered_dimensions.remove(0));
                    }
                }
                assert!(filtered_dimensions.is_empty());
                assert!(unfiltered_dimensions.is_empty());

                let dim_plans: Vec<LogicalPlan> =
                    result.iter().map(|rel| rel.plan.clone()).collect();
                let optimized = if facts.len() == 1 {
                    build_join_tree(&facts[0].plan, &dim_plans, &mut conds)?
                } else {
                    // build one join tree for each fact table
                    let fact_dim_joins = facts
                        .iter()
                        .map(|f| build_join_tree(&f.plan, &dim_plans, &mut conds))
                        .collect::<Result<Vec<_>>>()?;
                    // join the trees together
                    build_join_tree(&fact_dim_joins[0], &fact_dim_joins[1..], &mut conds)?
                };

                if conds.is_empty() {
                    println!("Optimized: {}", optimized.display_indent());
                    return Ok(Some(optimized));
                } else {
                    println!("Did not use all join conditions");
                    return Ok(None);
                }
            }
            _ => {
                println!("not a join");
                // TODO do we need to manually recurse here
                Ok(Some(utils::optimize_children(self, plan, _config)?))
            }
        }
    }

    fn optimize(&self, _plan: &LogicalPlan, _config: &mut OptimizerConfig) -> Result<LogicalPlan> {
        // this method is not needed because we implement try_optimize instead
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
            .filter(|e| match e {
                Expr::IsNotNull(_) => false,
                _ => true,
            })
            .count();
        x > 0
    }

    match plan {
        LogicalPlan::Filter(filter) => is_real_filter(filter.predicate()),
        LogicalPlan::TableScan(scan) => scan.filters.iter().any(is_real_filter),
        _ => plan.inputs().iter().any(|child| has_filter(child)),
    }
}

/// Extracts items of consecutive inner joins and join conditions.
/// This method works for bushy trees and left/right deep trees.
fn extract_inner_joins(plan: &LogicalPlan) -> (Vec<LogicalPlan>, HashSet<(Column, Column)>) {
    fn _extract_inner_joins(
        plan: &LogicalPlan,
        rels: &mut Vec<LogicalPlan>,
        conds: &mut HashSet<(Column, Column)>,
    ) {
        match plan {
            LogicalPlan::Join(join)
                if join.join_type == JoinType::Inner && join.filter.is_none() =>
            {
                _extract_inner_joins(&join.left, rels, conds);
                _extract_inner_joins(&join.right, rels, conds);
                // TODO could also handle join conditions here?

                for (l, r) in &join.on {
                    conds.insert((l.clone(), r.clone()));
                }
            }
            _ => {
                if find_join(plan).is_some() {
                    for x in plan.inputs() {
                        _extract_inner_joins(x, rels, conds);
                    }
                } else {
                    // leaf node
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
    //TODO check for deterministic join/filter expressions

    fn is_supported_rel(plan: &LogicalPlan) -> bool {
        // println!("is_simple_rel? {}", plan.display_indent());
        match plan {
            LogicalPlan::Join(join) => {
                join.join_type == JoinType::Inner
                    && join.filter.is_none()
                    && is_supported_rel(&join.left)
                    && is_supported_rel(&join.right)
            }
            LogicalPlan::Filter(filter) => is_supported_rel(filter.input()),
            LogicalPlan::SubqueryAlias(sq) => is_supported_rel(&sq.input),
            LogicalPlan::TableScan(_) => true,
            _ => {
                println!("not a simple join: {}", plan.display_indent());
                false
            }
        }
    }

    is_supported_rel(&LogicalPlan::Join(join.clone()))
}

/// find first (top-level) join in plan
fn find_join(plan: &LogicalPlan) -> Option<Join> {
    match plan {
        LogicalPlan::Join(join) => Some(join.clone()),
        other => {
            if other.inputs().len() == 0 {
                None
            } else {
                for input in &other.inputs() {
                    if let Some(join) = find_join(*input) {
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
        // find join keys between the fact and this dim
        let mut join_keys = vec![];
        for (l, r) in conds.iter() {
            if (fact.schema().index_of_column(l).is_ok() && dim.schema().index_of_column(r).is_ok())
                || fact.schema().index_of_column(r).is_ok()
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

            println!("Joining fact to dim on {:?} = {:?}", left_keys, right_keys);
            b = b.join(&dim, JoinType::Inner, (left_keys, right_keys), None)?;
        }
    }
    b.build()
}

fn get_table_size(plan: &LogicalPlan) -> Option<usize> {
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
                // TODO hard-coded stats for manual testing until stats are available
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
                        println!("No row count available for table '{}'", other);
                        100
                    }
                };

                Some(n)
            }
        }
        _ => get_table_size(&plan.inputs()[0]),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{Result, Statistics};
    use datafusion_expr::{col, lit, JoinType, LogicalPlan, LogicalPlanBuilder, SubqueryAlias};

    use super::*;
    use crate::sql::table::DaskTableSource;

    #[test]
    fn inner_join_simple() -> Result<()> {
        let a = test_table_scan("t1", 100);
        let b = test_table_scan("t2", 100);
        let join = LogicalPlanBuilder::from(a)
            .join(&b, JoinType::Inner, (vec!["t1_a"], vec!["t2_b"]), None)?
            .build()?;
        if let LogicalPlan::Join(join) = join {
            assert!(is_supported_join(&join));
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
            assert!(!is_supported_join(&join));
        } else {
            panic!();
        }
        Ok(())
    }

    #[test]
    fn test_extract_inner_joins() -> Result<()> {
        let join = create_test_plan()?;
        let (rels, conds) = extract_inner_joins(&join);
        assert_eq!(4, rels.len());
        assert_eq!(3, conds.len());
        assert_eq!("TableScan: fact", &format!("{:?}", rels[0]));
        assert_eq!("TableScan: dim1", &format!("{:?}", rels[1]));
        assert_eq!("TableScan: dim2", &format!("{:?}", rels[2]));
        assert_eq!(
            "Filter: dim3.dim3_b <= Int32(100)\n  TableScan: dim3",
            &format!("{:?}", rels[3])
        );
        Ok(())
    }

    #[test]
    fn optimize_joins() -> Result<()> {
        let plan = create_test_plan()?;
        let formatted_plan = format!("{}", plan.display_indent());
        let expected_plan = r#"Inner Join: fact.fact_d = dim3.dim3_a
  Inner Join: fact.fact_c = dim2.dim2_a
    Inner Join: fact.fact_b = dim1.dim1_a
      TableScan: fact
      TableScan: dim1
    TableScan: dim2
  Filter: dim3.dim3_b <= Int32(100)
    TableScan: dim3"#;
        assert_eq!(expected_plan, formatted_plan);
        let rule = JoinReorder::default();
        let mut config = OptimizerConfig::default();
        let optimized_plan = rule.try_optimize(&plan, &mut config)?.unwrap();
        let formatted_plan = format!("{}", optimized_plan.display_indent());
        let expected_plan = r#"Inner Join: fact.fact_c = dim2.dim2_a
  Inner Join: fact.fact_b = dim1.dim1_a
    Inner Join: fact.fact_d = dim3.dim3_a
      TableScan: fact
      Filter: dim3.dim3_b <= Int32(100)
        TableScan: dim3
    TableScan: dim1
  TableScan: dim2"#;
        assert_eq!(expected_plan, formatted_plan);
        Ok(())
    }

    fn create_test_plan() -> Result<LogicalPlan> {
        let dim1 = test_table_scan("dim1", 100);
        let dim2 = test_table_scan("dim2", 200);
        let dim3 = test_table_scan("dim3", 50);
        let fact = test_table_scan("fact", 10000);

        // add a filter to one dimension
        let dim3 = LogicalPlanBuilder::from(dim3)
            .filter(col("dim3_b").lt_eq(lit(100)))?
            .build()?;

        LogicalPlanBuilder::from(fact)
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
            .build()
    }

    fn create_test_plan_with_aliases() -> Result<LogicalPlan> {
        let dim1 = aliased_plan(test_table_scan("date_dim", 100), "dim1");
        let dim2 = aliased_plan(test_table_scan("date_dim", 200), "dim2");
        let dim3 = aliased_plan(test_table_scan("date_dim", 300), "dim3");
        let fact = test_table_scan("fact", 10000);
        LogicalPlanBuilder::from(fact)
            .join(
                &dim1,
                JoinType::Inner,
                (vec!["fact_b"], vec!["date_dim_a"]),
                None,
            )?
            .join(
                &dim2,
                JoinType::Inner,
                (vec!["fact_c"], vec!["date_dim_a"]),
                None,
            )?
            .join(
                &dim3,
                JoinType::Inner,
                (vec!["fact_d"], vec!["date_dim_a"]),
                None,
            )?
            .build()
    }

    fn aliased_plan(plan: LogicalPlan, alias: &str) -> LogicalPlan {
        let schema = plan.schema().as_ref().clone();
        let schema = schema.replace_qualifier(alias);
        LogicalPlan::SubqueryAlias(SubqueryAlias {
            input: Arc::new(plan),
            alias: alias.to_string(),
            schema: Arc::new(schema),
        })
    }

    fn test_table_scan(table_name: &str, size: usize) -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new(&format!("{}_a", table_name), DataType::UInt32, false),
            Field::new(&format!("{}_b", table_name), DataType::UInt32, false),
            Field::new(&format!("{}_c", table_name), DataType::UInt32, false),
            Field::new(&format!("{}_d", table_name), DataType::UInt32, false),
        ]);
        table_scan(Some(table_name), &schema, None, size)
            .expect("creating scan")
            .build()
            .expect("building plan")
    }

    fn table_scan(
        name: Option<&str>,
        table_schema: &Schema,
        projection: Option<Vec<usize>>,
        table_size: usize,
    ) -> Result<LogicalPlanBuilder> {
        let tbl_schema = Arc::new(table_schema.clone());
        let mut statistics = Statistics::default();
        statistics.num_rows = Some(table_size);
        let table_source = Arc::new(DaskTableSource::new_with_statistics(
            tbl_schema,
            Some(statistics),
        ));
        LogicalPlanBuilder::scan(name.unwrap_or("test"), table_source, projection)
    }
}
