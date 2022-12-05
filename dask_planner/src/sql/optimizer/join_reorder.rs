//! Join reordering based on the paper "Improving Join Reordering for Large Scale Distributed Computing"
//! https://ieeexplore.ieee.org/document/9378281

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use datafusion_common::{Column, DFSchema, Result};
use datafusion_expr::{EmptyRelation, Join, JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion_optimizer::{utils, OptimizerConfig, OptimizerRule};

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
        println!("JoinReorder::try_optimize()");

        match plan {
            LogicalPlan::Join(join) if join.join_type == JoinType::Inner => {
                // we can only reorder simple joins, as defined in `is_simple_join`
                if !is_simple_join(join) {
                    println!("Not a simple join");
                    return Ok(None);
                }
                println!("simple join");

                // get a list of joins, un-nested
                let joins = unnest_joins(join);

                let (fact, dims) = extract_fact_dimensions(&joins);
                if fact.is_none() {
                    println!("There is no dominant fact table");
                    return Ok(None);
                }
                let fact = fact.unwrap(); // unwrap here is safe due to previous check

                let tree_type = tree_type(join);
                let n = dims.len();

                let mut unfiltered_dimensions = get_unfiltered_dimensions(&dims);
                unfiltered_dimensions.push(JoinInput::dummy(usize::MAX));

                let mut filtered_dimensions = get_filtered_dimensions(&dims);
                filtered_dimensions.push(JoinInput::dummy(usize::MIN));
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
                let optimized = build_join_tree(tree_type, &joins, &fact, &result).unwrap(); // TODO use ?

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
    name: String,
    plan: LogicalPlan,
    size: usize,
}

impl JoinInput {
    /// Create a dummy table with infinite size
    fn dummy(size: usize) -> Self {
        Self {
            name: "_dummy_".to_string(),
            plan: LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(DFSchema::empty()),
            }),
            size,
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    /// Determine if this plan contains any filters
    fn has_filter(&self) -> bool {
        match &self.plan {
            LogicalPlan::Filter(_) => true,
            LogicalPlan::TableScan(scan) => !scan.filters.is_empty(),
            _ => false,
        }
    }
}

/// Simple join between two relations (no nested joins)
#[derive(Debug)]
struct SimpleJoin {
    left: JoinInput,
    right: JoinInput,
    on: Vec<(Column, Column)>,
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
                // TODO until stats are actually available
                let n = match scan.table_name.as_str() {
                    "catalog_returns" => 4_000_000,
                    "catalog_sales" => 35_000_000,
                    "customer_demographics" => 35_000,
                    "date_dim" => 3000,
                    "household_demographics" => 500,
                    "inventory" => 116_000_000,
                    "item" => 29_000,
                    "promotion" => 100,
                    "warehouse" => 10,
                    _ => 100,
                };

                Some(n)
            }
        }
        _ => get_table_size(&plan.inputs()[0]),
    }
}

/// build a list of all joins
fn unnest_joins(join: &Join) -> Vec<SimpleJoin> {
    fn unnest_joins_inner(
        plan: &LogicalPlan,
        joins: &mut Vec<SimpleJoin>,
        left_count: &mut usize,
        right_count: &mut usize,
    ) {
        //TODO increment left_count and right_count depending on which side the nested join
        // is, so we can determine if the join is LeftDeep, RightDeep, or something else

        match plan {
            LogicalPlan::Join(join) => {
                let mut left_name = None;
                let mut left_plan = None;
                let mut right_name = None;
                let mut right_plan = None;

                if join.filter.is_some() {
                    //TODO cannot just drop join filters
                    todo!()
                }

                for (l, r) in &join.on {
                    // left and right could be in either order but we need to make sure the condition is between left and right
                    if let Some((left_table_name, left_subplan)) = resolve_table_plan(&join.left, l)
                    {
                        if let Some((right_table_name, right_subplan)) =
                            resolve_table_plan(&join.right, r)
                        {
                            //TODO if overwriting existing plan, check they match first
                            left_name = Some(left_table_name);
                            left_plan = Some(left_subplan);
                            right_name = Some(right_table_name);
                            right_plan = Some(right_subplan);
                        } else {
                            println!("NOT FOUND: left={}, right={}", l, r);
                            todo!()
                        }
                    } else if let Some((left_table_name, left_subplan)) =
                        resolve_table_plan(&join.right, l)
                    {
                        if let Some((right_table_name, right_subplan)) =
                            resolve_table_plan(&join.left, r)
                        {
                            //TODO if overwriting existing plan, check they match first
                            left_name = Some(left_table_name);
                            left_plan = Some(left_subplan);
                            right_name = Some(right_table_name);
                            right_plan = Some(right_subplan);
                        } else {
                            println!("NOT FOUND: left={}, right={}", l, r);
                            todo!()
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

                println!("JOIN: {:?}", simple_join);

                joins.push(simple_join);

                unnest_joins_inner(&join.left, joins, left_count, right_count);
                unnest_joins_inner(&join.right, joins, left_count, right_count);
            }
            other => {
                for child in other.inputs() {
                    unnest_joins_inner(child, joins, left_count, right_count);
                }
            }
        }
    }

    let mut left_count = 0;
    let mut right_count = 0;
    let mut joins = vec![];
    unnest_joins_inner(
        &LogicalPlan::Join(join.clone()),
        &mut joins,
        &mut left_count,
        &mut right_count,
    );

    println!("nest counts: left={}, right={}", left_count, right_count);

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

fn tree_type(_join: &Join) -> TreeType {
    //TODO implement
    TreeType::LeftDeep
}

/// Extract the fact table from the join, along with a list of dimension tables that
/// join to the fact table
fn extract_fact_dimensions(joins: &[SimpleJoin]) -> (Option<JoinInput>, Vec<JoinInput>) {
    // at least half of joins should be inner joins involving one common table (the fact table)
    // other tables being joined with fact table are considered dimension tables

    let mut table_names_unique = HashSet::new();
    let mut table_names = vec![];
    let mut table_sizes = HashMap::new();
    for join in joins {
        table_names_unique.insert(join.left.name.clone());
        table_names_unique.insert(join.right.name.clone());
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
/// column (which is used in a join expression)
fn resolve_table_plan(plan: &LogicalPlan, col: &Column) -> Option<(String, LogicalPlan)> {
    println!(
        "Looking for column {} in plan: {}",
        col,
        plan.display_indent()
    );

    match plan {
        LogicalPlan::TableScan(scan) => {
            if let Ok(_) = scan.projected_schema.index_of_column(col) {
                Some((scan.table_name.clone(), plan.clone()))
            } else {
                None
            }
        }
        LogicalPlan::Join(join) => {
            let ll = resolve_table_plan(&join.left, col);
            let rr = resolve_table_plan(&join.right, col);
            if ll.is_some() && rr.is_some() {
                // ambiguous
                None
            } else if ll.is_some() {
                ll
            } else if rr.is_some() {
                rr
            } else {
                None
            }
        }
        LogicalPlan::SubqueryAlias(alias) => match &col.relation {
            Some(r) if alias.alias == *r => {
                let mut x = col.clone();
                x.relation = None;
                resolve_table_plan(&alias.input, &x)
            }
            _ => None,
        },
        _ => {
            if plan.inputs().is_empty() {
                None
            } else {
                let table_plan = resolve_table_plan(plan.inputs()[0], col);

                //TODO reimplement this so that we don't drop filters wrapping table scans

                return table_plan;
                // match table_plan {
                //     Some((name, _)) => {
                //         // return the entire sub-plan that contains the relation (but no joins)
                //
                //         // TODO remove this debug assertion
                //         if plan_contains_join(plan) {
                //             panic!("failed to unnest join");
                //         }
                //
                //         Some((name, plan.clone()))
                //     }
                //     None => None,
                // }
            }
        }
    }
}

fn plan_contains_join(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Join(_) => true,
        other => {
            if other.inputs().len() == 0 {
                false
            } else {
                plan_contains_join(&other.inputs()[0])
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
    _tree_type: TreeType,
    joins: &[SimpleJoin],
    fact: &JoinInput,
    dims: &[JoinInput],
) -> Result<LogicalPlan> {
    println!(
        "build_join_tree() fact={}, dims={:?}",
        fact.name,
        dims.iter().map(|d| d.name()).collect::<Vec<&str>>()
    );

    //TODO respect tree_type

    println!("fact schema: {:?}", fact.plan.schema().field_names());

    let mut b = LogicalPlanBuilder::from(fact.plan.clone());
    let mut prev_table_name = fact.name().to_owned();

    for dim in dims {
        println!("dim schema: {:?}", dim.plan.schema().field_names());

        let mut join_keys = vec![];

        for join in joins {
            if join.left.name == prev_table_name && join.right.name == dim.name() {
                join_keys = join.on.clone();
            } else if join.right.name == prev_table_name && join.left.name == dim.name() {
                join_keys = join.on.clone();
            }
        }

        let left_keys: Vec<Column> = join_keys.iter().map(|(l, _r)| l.clone()).collect();
        let right_keys: Vec<Column> = join_keys.iter().map(|(_l, r)| r.clone()).collect();

        println!(
            "Joining {} to {} on {:?} = {:?}",
            prev_table_name,
            dim.name(),
            left_keys,
            right_keys
        );

        b = b.join(&dim.plan, JoinType::Inner, (left_keys, right_keys), None)?;

        prev_table_name = dim.name().to_owned();
    }

    b.build()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{Result, Statistics};
    use datafusion_expr::{JoinType, LogicalPlan, LogicalPlanBuilder};

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
            let joins = unnest_joins(&join);
            assert_eq!(3, joins.len());
        } else {
            panic!()
        }
        Ok(())
    }

    // #[test]
    // fn test_extract_fact_dimension() -> Result<()> {
    //     let plan = create_test_plan()?;
    //     if let LogicalPlan::Join(ref join) = plan {
    //         let joins = unnest_joins(&join);
    //         let _ = extract_fact_dimensions(&plan, &joins);
    //     } else {
    //         panic!()
    //     }
    //     Ok(())
    // }

    // #[test]
    // fn test_resolve_columns() -> Result<()> {
    //     let plan = create_test_plan()?;
    //     assert_eq!(
    //         "fact",
    //         resolve_table_plan(&plan, &create_column("fact_b")).unwrap()
    //     );
    //     assert_eq!(
    //         "fact",
    //         resolve_table_plan(&plan, &create_column("fact_c")).unwrap()
    //     );
    //     assert_eq!(
    //         "fact",
    //         resolve_table_plan(&plan, &create_column("fact_d")).unwrap()
    //     );
    //     assert_eq!(
    //         "dim1",
    //         resolve_table_plan(&plan, &create_column("dim1_a")).unwrap()
    //     );
    //     assert_eq!(
    //         "dim2",
    //         resolve_table_plan(&plan, &create_column("dim2_a")).unwrap()
    //     );
    //     assert_eq!(
    //         "dim3",
    //         resolve_table_plan(&plan, &create_column("dim3_a")).unwrap()
    //     );
    //     Ok(())
    // }

    fn create_column(name: &str) -> Column {
        Column::new(None::<String>, name.to_owned())
    }

    fn create_test_plan() -> Result<LogicalPlan> {
        let dim1 = test_table_scan("dim1", 100);
        let dim2 = test_table_scan("dim2", 200);
        let dim3 = test_table_scan("dim3", 300);
        let fact = test_table_scan("fact", 10000);
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
