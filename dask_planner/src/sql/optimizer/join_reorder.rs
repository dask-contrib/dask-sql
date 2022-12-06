//! Join reordering based on the paper "Improving Join Reordering for Large Scale Distributed Computing"
//! https://ieeexplore.ieee.org/document/9378281

use std::{collections::HashMap, sync::Arc};

use datafusion_common::{Column, DataFusionError, Result};
use datafusion_expr::{Expr, Join, JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion_optimizer::{utils, utils::split_conjunction, OptimizerConfig, OptimizerRule};

use crate::sql::table::DaskTableSource;

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
        match plan {
            LogicalPlan::Join(join) if join.join_type == JoinType::Inner => {
                // we can only reorder simple joins, as defined in `is_simple_join`
                if !is_simple_join(join) {
                    println!("Not a simple join");
                    return Ok(None);
                }
                println!(
                    "JoinReorder attempting to optimize join: {}",
                    plan.display_indent()
                );

                // get a list of joins, un-nested
                let (tree_type, joins) = unnest_joins(join)?;
                for join in &joins {
                    println!("Join: {:?}", join)
                }

                let (fact, dims) = extract_fact_dimensions(&joins);
                if fact.is_none() {
                    println!("There is no dominant fact table");
                    return Ok(None);
                }
                let fact = fact.unwrap(); // unwrap here is safe due to previous check

                let n = dims.len();

                let mut unfiltered_dimensions = get_unfiltered_dimensions(&dims);
                let mut filtered_dimensions = get_filtered_dimensions(&dims);
                filtered_dimensions.sort_by(|a, b| a.size.cmp(&b.size));

                for dim in &unfiltered_dimensions {
                    println!(
                        "UNFILTERED: {} {} {}",
                        dim.name,
                        dim.size,
                        dim.plan.display_indent()
                    );
                }

                for dim in &filtered_dimensions {
                    println!(
                        "FILTERED: {} {} {}",
                        dim.name,
                        dim.size,
                        dim.plan.display_indent()
                    );
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
                for _ in 0..n {
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

                let optimized = build_join_tree(tree_type, &joins, &fact, &result)?;

                println!("Optimized: {}", optimized.display_indent());

                return Ok(Some(optimized));
            }
            _ => {
                println!("not a join");
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
struct JoinInput {
    /// Name of the fact or dimension table represented by this join input
    name: String,
    /// Plan containing the table scan for the fact or dimension table. May also contain
    /// Filter and SubqueryAlias.
    plan: LogicalPlan,
    /// Estimated size of the underlying table before any filtering is applied
    size: usize,
}

impl JoinInput {
    /// Get the name of the fact or dimension table represented by this join input
    fn name(&self) -> &str {
        &self.name
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

/// Simple join between two relations (no nested joins)
#[derive(Debug)]
struct SimpleJoin {
    left: JoinInput,
    right: JoinInput,
    on: Vec<(Column, Column)>,
    //TODO track join filters here as well ?
}

#[derive(Debug)]
enum TreeType {
    LeftDeep,
    RightDeep,
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

/// build a list of all joins
fn unnest_joins(join: &Join) -> Result<(TreeType, Vec<SimpleJoin>)> {
    fn unnest_joins_inner(
        plan: &LogicalPlan,
        joins: &mut Vec<SimpleJoin>,
        left_count: &mut usize,
        right_count: &mut usize,
    ) -> Result<()> {
        //TODO increment left_count and right_count depending on which side the nested join
        // is, so we can determine if the join is LeftDeep, RightDeep, or something else

        match plan {
            LogicalPlan::Join(join) => {
                let mut left_name = None;
                let mut left_plan = None;
                let mut right_name = None;
                let mut right_plan = None;

                if join.filter.is_some() {
                    return Err(DataFusionError::Plan(
                        "No support for joins with filters yet".to_string(),
                    ));
                }

                for (l, r) in &join.on {
                    // left and right could be in either order but we need to make sure the condition is between left and right
                    if let Some((left_table_name, left_subplan)) =
                        resolve_table_plan(&join.left, l)?
                    {
                        if let Some((right_table_name, right_subplan)) =
                            resolve_table_plan(&join.right, r)?
                        {
                            //TODO if overwriting existing plan, check they match first
                            left_name = Some(left_table_name);
                            left_plan = Some(left_subplan);
                            right_name = Some(right_table_name);
                            right_plan = Some(right_subplan);
                        } else {
                            return Err(DataFusionError::Plan(format!(
                                "Failed to unnest join: left={}, right={}",
                                l, r
                            )));
                        }
                    } else if let Some((left_table_name, left_subplan)) =
                        resolve_table_plan(&join.right, l)?
                    {
                        if let Some((right_table_name, right_subplan)) =
                            resolve_table_plan(&join.left, r)?
                        {
                            //TODO if overwriting existing plan, check they match first
                            left_name = Some(left_table_name);
                            left_plan = Some(left_subplan);
                            right_name = Some(right_table_name);
                            right_plan = Some(right_subplan);
                        } else {
                            return Err(DataFusionError::Plan(format!(
                                "Failed to unnest join: left={}, right={}",
                                l, r
                            )));
                        }
                    }
                }

                let ll_size = get_table_size(left_plan.as_ref().unwrap()).unwrap();
                let rr_size = get_table_size(right_plan.as_ref().unwrap()).unwrap();
                let simple_join = SimpleJoin {
                    left: JoinInput {
                        name: left_name.unwrap(),
                        plan: left_plan.unwrap().clone(),
                        size: ll_size,
                    },
                    right: JoinInput {
                        name: right_name.unwrap(),
                        plan: right_plan.unwrap().clone(),
                        size: rr_size,
                    },
                    on: join.on.clone(),
                };

                // println!("JOIN: {:?}", simple_join);

                unnest_joins_inner(&join.left, joins, left_count, right_count)?;
                unnest_joins_inner(&join.right, joins, left_count, right_count)?;

                joins.push(simple_join);
            }
            other => {
                for child in other.inputs() {
                    unnest_joins_inner(child, joins, left_count, right_count)?;
                }
            }
        }
        Ok(())
    }

    let mut left_count = 0;
    let mut right_count = 0;
    let mut joins = vec![];
    unnest_joins_inner(
        &LogicalPlan::Join(join.clone()),
        &mut joins,
        &mut left_count,
        &mut right_count,
    )?;

    //println!("nest counts: left={}, right={}", left_count, right_count);

    //TODO do not hard-code TreeType
    Ok((TreeType::LeftDeep, joins))
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
        // println!("is_simple_rel? {}", plan.display_indent());
        match plan {
            LogicalPlan::Join(join) => {
                join.join_type == JoinType::Inner
                    && is_simple_rel(&join.left)
                    && is_simple_rel(&join.right)
            }
            LogicalPlan::Filter(filter) => is_simple_rel(filter.input()),
            LogicalPlan::SubqueryAlias(sq) => is_simple_rel(&sq.input),
            LogicalPlan::TableScan(_) => true,
            _ => {
                println!("not a simple join: {}", plan.display_indent());
                false
            }
        }
    }

    is_simple_rel(&LogicalPlan::Join(join.clone()))
}

/// Extract the fact table from the join, along with a list of dimension tables that
/// join to the fact table
fn extract_fact_dimensions(joins: &[SimpleJoin]) -> (Option<JoinInput>, Vec<JoinInput>) {
    // at least half of joins should be inner joins involving one common table (the fact table)
    // other tables being joined with fact table are considered dimension tables

    // use Vec rather than HashSet to build list of unique table names because
    // we need to preserve the order they appear in joins
    let mut table_names_unique: Vec<String> = vec![];

    let mut table_names = vec![];
    let mut table_sizes = HashMap::new();
    for join in joins {
        if !table_names_unique.contains(&join.left.name) {
            table_names_unique.push(join.left.name.clone());
        }
        if !table_names_unique.contains(&join.right.name) {
            table_names_unique.push(join.right.name.clone());
        }
        table_names.push(join.left.name.clone());
        table_names.push(join.right.name.clone());
        table_sizes.insert(&join.left.name, join.left.size);
        table_sizes.insert(&join.right.name, join.right.size);
    }

    // detect fact and dimension tables
    let mut fact_table = None;
    let mut fact_table_size = 0_usize;

    for table_name in &table_names_unique {
        let count = table_names
            .iter()
            .filter(|name| *name == table_name)
            .count();
        println!("{} = {}", table_name, count);
        if count >= joins.len() / 2 {
            let size = *table_sizes.get(table_name).unwrap_or(&0_usize);

            println!("candidate fact table {} with size {}", table_name, size);
            if fact_table.is_none() || size > fact_table_size {
                fact_table = Some(table_name.clone());
                fact_table_size = size;
            }
        }
    }

    if let Some(fact_table) = fact_table {
        let dim_tables: Vec<String> = table_names_unique
            .iter()
            .filter(|name| *name != &fact_table)
            .cloned()
            .collect();
        println!("fact table: {:?}", fact_table);
        println!("dimension tables: {:?}", dim_tables);

        //TODO check fact_dimension_ratio
        let fact_table = get_plan(joins, &fact_table);
        let dim_tables = dim_tables
            .iter()
            .map(|name| get_plan(joins, &name).unwrap())
            .collect();
        (fact_table, dim_tables)
    } else {
        (None, vec![])
    }
}

fn get_plan(joins: &[SimpleJoin], name: &str) -> Option<JoinInput> {
    for join in joins {
        if join.left.name == name {
            return Some(join.left.clone());
        } else if join.right.name == name {
            return Some(join.right.clone());
        }
    }
    None
}

/// Find the leaf sub-plan in a join that contains the relation referenced by the specific
/// column (which is used in a join expression), along with any Filter or SubqueryAlias nodes.
///
/// The returned plan should not contain any joins.
fn resolve_table_plan(plan: &LogicalPlan, col: &Column) -> Result<Option<(String, LogicalPlan)>> {
    // println!(
    //     "Looking for column {} in plan: {}",
    //     col,
    //     plan.display_indent()
    // );

    fn get_table_scan_name(plan: &LogicalPlan, col: &Column) -> Option<String> {
        match plan {
            LogicalPlan::TableScan(scan) => {
                if let Ok(_) = scan.projected_schema.index_of_column(col) {
                    Some(scan.table_name.clone())
                } else {
                    None
                }
            }
            LogicalPlan::SubqueryAlias(alias) => match &col.relation {
                Some(r) if *r == alias.alias => {
                    let mut aliased_column = col.clone();
                    aliased_column.relation = None;
                    if let Some(_) = get_table_scan_name(&alias.input, &aliased_column) {
                        Some(alias.alias.clone())
                    } else {
                        None
                    }
                }
                _ => None,
            },
            _ => {
                for input in &plan.inputs() {
                    if let Some(x) = get_table_scan_name(input, col) {
                        return Some(x);
                    }
                }
                None
            }
        }
    }

    let mut candidate_plan = Arc::new(plan.clone());
    loop {
        let scan_name = get_table_scan_name(&candidate_plan, col);
        if scan_name.is_some() {
            if let Some(join) = find_join(&candidate_plan) {
                if get_table_scan_name(&join.left, col).is_some() {
                    candidate_plan = join.left.clone();
                } else if get_table_scan_name(&join.right, col).is_some() {
                    candidate_plan = join.right.clone();
                }
            } else {
                println!(
                    "found plan for {}: {}",
                    col,
                    candidate_plan.display_indent()
                );
                return Ok(Some((scan_name.unwrap(), candidate_plan.as_ref().clone())));
            }
        } else {
            return Ok(None);
        }
    }
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

fn get_unfiltered_dimensions(dims: &[JoinInput]) -> Vec<JoinInput> {
    dims.iter().filter(|t| !t.has_filter()).cloned().collect()
}

fn get_filtered_dimensions(dims: &[JoinInput]) -> Vec<JoinInput> {
    dims.iter().filter(|t| t.has_filter()).cloned().collect()
}

fn build_join_tree(
    tree_type: TreeType,
    joins: &[SimpleJoin],
    fact: &JoinInput,
    dims: &[JoinInput],
) -> Result<LogicalPlan> {
    println!(
        "build_join_tree() fact={}, dims={:?}",
        fact.name,
        dims.iter().map(|d| d.name()).collect::<Vec<&str>>()
    );

    // println!("fact schema: {:?}", fact.plan.schema().field_names());

    let mut b = LogicalPlanBuilder::from(fact.plan.clone());

    let mut dims_indirect_join = vec![];

    for dim in dims {
        // println!("dim schema: {:?}", dim.plan.schema().field_names());

        let mut join_keys = vec![];

        for join in joins {
            // println!(
            //     "inspecting join from {} to {}",
            //     join.left.name, join.right.name
            // );
            if join.left.name == fact.name() && join.right.name == dim.name() {
                join_keys = join.on.clone();
            } else if join.right.name == fact.name() && join.left.name == dim.name() {
                join_keys = join.on.clone();
            }
        }

        if join_keys.is_empty() {
            // this happens when the original join does not contain a direct join between
            // the fact table and this dimension table
            dims_indirect_join.push(dim);
        } else {
            let left_keys: Vec<Column> = join_keys.iter().map(|(l, _r)| l.clone()).collect();
            let right_keys: Vec<Column> = join_keys.iter().map(|(_l, r)| r.clone()).collect();

            println!(
                "Joining {} to {} on {:?} = {:?}",
                fact.name(),
                dim.name(),
                left_keys,
                right_keys
            );

            match tree_type {
                TreeType::LeftDeep => {
                    b = b.join(&dim.plan, JoinType::Inner, (left_keys, right_keys), None)?;
                }
                TreeType::RightDeep => {
                    b = LogicalPlanBuilder::from(dim.plan.clone()).join(
                        &b.build()?,
                        JoinType::Inner,
                        (left_keys, right_keys),
                        None,
                    )?;
                }
            }
        }
    }

    // add remaining joins
    for dim in dims_indirect_join {
        let mut join_keys = vec![];

        for join in joins {
            // println!(
            //     "inspecting join from {} to {}",
            //     join.left.name, join.right.name
            // );
            if join.left.name == dim.name() || join.right.name == dim.name() {
                join_keys = join.on.clone();
            }
        }

        if join_keys.is_empty() {
            return Err(DataFusionError::Plan(
                "Could not determine join keys".to_string(),
            ));
        }
        let left_keys: Vec<Column> = join_keys.iter().map(|(l, _r)| l.clone()).collect();
        let right_keys: Vec<Column> = join_keys.iter().map(|(_l, r)| r.clone()).collect();

        match tree_type {
            TreeType::LeftDeep => {
                b = b.join(&dim.plan, JoinType::Inner, (left_keys, right_keys), None)?;
            }
            TreeType::RightDeep => {
                b = LogicalPlanBuilder::from(dim.plan.clone()).join(
                    &b.build()?,
                    JoinType::Inner,
                    (left_keys, right_keys),
                    None,
                )?;
            }
        }
    }

    b.build()
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
    fn test_unnest_joins() -> Result<()> {
        let join = create_test_plan()?;
        if let LogicalPlan::Join(join) = join {
            let (_tree_type, joins) = unnest_joins(&join)?;
            assert_eq!(3, joins.len());

            assert_eq!("SimpleJoin { \
                left: JoinInput { name: \"fact\", plan: TableScan: fact, size: 10000 }, \
                right: JoinInput { name: \"dim1\", plan: TableScan: dim1, size: 100 }, \
                on: [(Column { relation: Some(\"fact\"), name: \"fact_b\" }, Column { relation: Some(\"dim1\"), name: \"dim1_a\" })] }", &format!("{:?}", joins[0]));

            assert_eq!("SimpleJoin { \
                left: JoinInput { name: \"fact\", plan: TableScan: fact, size: 10000 }, \
                right: JoinInput { name: \"dim2\", plan: TableScan: dim2, size: 200 }, \
                on: [(Column { relation: Some(\"fact\"), name: \"fact_c\" }, Column { relation: Some(\"dim2\"), name: \"dim2_a\" })] }", &format!("{:?}", joins[1]));

            assert_eq!("SimpleJoin { \
                left: JoinInput { name: \"fact\", plan: TableScan: fact, size: 10000 }, \
                right: JoinInput { name: \"dim3\", plan: Filter: dim3.dim3_b <= Int32(100)
  TableScan: dim3, size: 50 }, \
                on: [(Column { relation: Some(\"fact\"), name: \"fact_d\" }, Column { relation: Some(\"dim3\"), name: \"dim3_a\" })] }", &format!("{:?}", joins[2]));
        } else {
            panic!()
        }
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

    #[test]
    fn test_extract_fact_dimension() -> Result<()> {
        let plan = create_test_plan()?;
        if let LogicalPlan::Join(ref join) = plan {
            let (_tree_type, joins) = unnest_joins(&join)?;
            let (fact, dims) = extract_fact_dimensions(&joins);
            assert_eq!("fact", fact.unwrap().name);
            let mut dim_names = dims.iter().map(|d| d.name()).collect::<Vec<&str>>();
            dim_names.sort();
            assert_eq!(vec!["dim1", "dim2", "dim3"], dim_names);
        } else {
            panic!()
        }
        Ok(())
    }

    #[test]
    fn test_resolve_columns() -> Result<()> {
        let plan = create_test_plan()?;

        fn test(plan: &LogicalPlan, column_name: &str, expected_table_name: &str) -> Result<()> {
            let col = Column::new(None::<String>, column_name.to_owned());
            let (name, _) = resolve_table_plan(&plan, &col)?.unwrap();
            assert_eq!(name, expected_table_name);
            Ok(())
        }

        test(&plan, "fact_b", "fact")?;
        test(&plan, "fact_c", "fact")?;
        test(&plan, "fact_d", "fact")?;
        test(&plan, "dim1_a", "dim1")?;
        test(&plan, "dim2_a", "dim2")?;
        test(&plan, "dim3_a", "dim3")?;
        Ok(())
    }

    #[test]
    fn test_resolve_aliased_columns() -> Result<()> {
        let plan = create_test_plan_with_aliases()?;

        fn test(plan: &LogicalPlan, column_name: &str, expected_table_name: &str) -> Result<()> {
            let col = Column::from(column_name);
            let (name, _) = resolve_table_plan(&plan, &col)?.unwrap();
            assert_eq!(name, expected_table_name);
            Ok(())
        }

        test(&plan, "fact_b", "fact")?;
        test(&plan, "fact_c", "fact")?;
        test(&plan, "fact_d", "fact")?;
        test(&plan, "dim1.date_dim_a", "dim1")?;
        test(&plan, "dim2.date_dim_a", "dim2")?;
        test(&plan, "dim3.date_dim_a", "dim3")?;
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
