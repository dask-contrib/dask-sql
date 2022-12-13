//! Optimizer rule dropping join key columns post join
//!
//! Many queries follow a pattern where certain columns are only used as join keys and aren't
//! really needed/selected in the query post join. In cases such as these it is useful to drop
//! those columns post join rather than carrying them around for subsequent operations and only
//! dropping them at the end during the projection.
//!
//! A query like
//! ```text
//! SELECT
//!     SUM(df.a), df2.b
//! FROM df
//! INNER JOIN df2
//!     ON df.c = df2.c
//! GROUP BY df2.b
//! ```
//!
//! Would typically produce a LogicalPlan like ...
//! ```text
//! Projection: SUM(df.a), df2.b\
//!   Aggregate: groupBy=[[df2.b]], aggr=[[SUM(df.a)]]\
//!     Inner Join: df.c = df2.c\
//!       TableScan: df projection=[a, c], full_filters=[df.c IS NOT NULL]\
//!       TableScan: df2 projection=[b, c], full_filters=[df2.c IS NOT NULL]\
//! ```
//!
//! Where df.c and df2.c would be unnecessarily carried into the aggregate step even though it can
//! be dropped.
//!
//! To solve this problem, we insert a projection after the join step. In our example, the
//! optimized LogicalPlan is
//! ```text
//! Projection: SUM(df.a), df2.b\
//!   Aggregate: groupBy=[[df2.b]], aggr=[[SUM(df.a)]]\
//!     Projection: df.a, df2.b\
//!       Inner Join: df.c = df2.c\
//!         TableScan: df projection=[a, c], full_filters=[df.c IS NOT NULL]\
//!         TableScan: df2 projection=[b, c], full_filters=[df2.c IS NOT NULL]\
//! ```
//!
//! Which corresponds to rewriting the query as
//! ```text
//! SELECT
//!     SUM(df.a), df2.b
//! FROM
//!     (SELECT
//!         df.a, df2.b
//!     FROM df
//!     INNER JOIN df2
//!         ON df.c = df2.c)
//! GROUP BY df2.b
//! ```
//!
//! Note that if the LogicalPlan was something like
//! ```text
//! Projection: df.a, df2.b\
//!   Inner Join: df.c = df2.c\
//!     TableScan: df projection=[a, c], full_filters=[df.c IS NOT NULL]\
//!     TableScan: df2 projection=[b, c], full_filters=[df2.c IS NOT NULL]\
//! ```
//!
//! Then we need not add an additional Projection step, as columns df.c and df2.c are immediately
//! dropped in the next step, which is itself a Projection.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use datafusion_common::{Column, DFSchema, Result};
use datafusion_expr::{
    logical_plan::{Analyze, CrossJoin, Join, LogicalPlan, Projection, Union},
    Aggregate,
    BinaryExpr,
    CreateMemoryTable,
    CreateView,
    Distinct,
    Explain,
    Expr,
    Filter,
    Limit,
    Repartition,
    Sort,
    Subquery,
    SubqueryAlias,
    Window,
};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use log::warn;

/// Optimizer rule dropping join key columns post join
#[derive(Default)]
pub struct FilterColumnsPostJoin {}

impl FilterColumnsPostJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for FilterColumnsPostJoin {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        _optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        // Store info about all columns in all schemas
        let all_schemas = &plan.all_schemas();
        optimize_top_down(plan, all_schemas, HashSet::new(), None)
    }

    fn name(&self) -> &str {
        "filter_columns_post_join"
    }
}

fn optimize_top_down(plan: &LogicalPlan, all_schemas: &Vec<&Arc<DFSchema>>, projected_columns: HashSet<Expr>, table_scan: Option<LogicalPlan>) -> Result<LogicalPlan> {
    let mut post_join_columns: HashSet<Expr> = projected_columns.clone();

    // For storing the steps of the LogicalPlan,
    // with an i32 key to keep track of the order
    let mut new_plan: HashMap<i32, LogicalPlan> = HashMap::new();

    // Iterate through LogicalPlan with &plan.inputs()
    let mut counter = 0;
    let mut current_plan = plan.clone();
    let mut join_flag = false;
    let mut previous_projection = false;

    loop {
        if current_plan.inputs().is_empty() {
            break;
        } else if current_plan.inputs().len() > 1 {
            match current_plan {
                LogicalPlan::Join(ref j) => {
                    join_flag = true;

                    // If every single potential_dropped_columns is in post_join_columns,
                    // then there's no use doing an additional projection
                    // because we would not be dropping any columns used by the Join
                    let mut should_project = false;
                    let current_columns = &current_plan.expressions();
                    let mut potential_dropped_columns = HashSet::new();
                    insert_post_join_columns(&mut potential_dropped_columns, current_columns);
                    for column in potential_dropped_columns {
                        if !post_join_columns.contains(&column) {
                            should_project = true;
                        }
                    }

                    // Check so that we don't build a stack of projections
                    if !previous_projection && should_project {
                        // Remove un-projectable columns
                        post_join_columns = filter_post_join_columns(&post_join_columns, table_scan);
                        // Remove duplicates from HashSet
                        post_join_columns = post_join_columns.iter().cloned().collect();
                        // Convert HashSet to Vector
                        let projection_columns: Vec<Expr> =
                            post_join_columns.clone().into_iter().collect();

                        // Prepare schema
                        let mut projection_schema_fields = vec![];
                        for column in &post_join_columns {
                            for dfschema in all_schemas {
                                let dfschema_fields = dfschema.fields();
                                for field in dfschema_fields {
                                    let field_expr = Expr::Column(Column::from_qualified_name(
                                        &field.qualified_name(),
                                    ));
                                    if (column == &field_expr)
                                        && !projection_schema_fields.contains(field)
                                    {
                                        projection_schema_fields.push(field.clone());
                                    }
                                }
                            }
                        }
                        let projection_schema =
                            DFSchema::new_with_metadata(projection_schema_fields, HashMap::new());

                        // Create a Projection with the columns from the HashSet
                        let projection_plan = LogicalPlan::Projection(Projection {
                            expr: projection_columns,
                            input: Arc::new(current_plan.clone()),
                            schema: Arc::new(projection_schema.unwrap()),
                            alias: None,
                        });

                        // Add Projection to HashMap
                        new_plan.insert(counter, projection_plan);
                        counter += 1;
                    }

                    let current_columns = &current_plan.expressions();
                    insert_post_join_columns(&mut post_join_columns, current_columns);

                    let mut left_table_join = None;
                    let mut right_table_join = None;
                    let j_left = &*j.left;
                    let j_right = &*j.right;
                    if let LogicalPlan::TableScan(_) = j_left {
                        right_table_join = Some(j_left.clone());
                    }
                    if let LogicalPlan::TableScan(_) = j_right {
                        left_table_join = Some(j_right.clone());
                    }
                    // Recurse on left and right inputs of Join
                    let left_join_plan = optimize_top_down(&j.left, all_schemas, post_join_columns.clone(), left_table_join).unwrap();
                    let right_join_plan = optimize_top_down(&j.right, all_schemas, post_join_columns.clone(), right_table_join).unwrap();
                    let join_plan = LogicalPlan::Join(Join {
                        left: Arc::new(left_join_plan),
                        right: Arc::new(right_join_plan),
                        on: j.on.clone(),
                        filter: j.filter.clone(),
                        join_type: j.join_type,
                        join_constraint: j.join_constraint,
                        schema: j.schema.clone(),
                        null_equals_null: j.null_equals_null,
                    });

                    // Add Join to HashMap
                    new_plan.insert(counter, join_plan);
                }
                LogicalPlan::CrossJoin(ref c) => {
                    // Recurse on left and right inputs of CrossJoin
                    let left_crossjoin_plan = optimize_top_down(&c.left, all_schemas, post_join_columns.clone(), None).unwrap();
                    let right_crossjoin_plan = optimize_top_down(&c.right, all_schemas, post_join_columns.clone(), None).unwrap();
                    let crossjoin_plan = LogicalPlan::CrossJoin(CrossJoin {
                        left: Arc::new(left_crossjoin_plan),
                        right: Arc::new(right_crossjoin_plan),
                        schema: c.schema.clone(),
                    });

                    // Add CrossJoin to HashMap
                    new_plan.insert(counter, crossjoin_plan);
                }
                LogicalPlan::Union(ref u) => {
                    // Recurse on inputs vector of Union
                    let mut new_inputs = vec![];
                    for input in &u.inputs {
                        let new_input = optimize_top_down(input, all_schemas, post_join_columns.clone(), None);
                        match new_input {
                            Ok(i) => new_inputs.push(Arc::new(i)),
                            _ => {
                                warn!("Skipping optimizer rule 'FilterColumnsPostJoin'");
                                return Ok(plan.clone());
                            }
                        }
                    }

                    let union_plan = LogicalPlan::Union(Union {
                        inputs: new_inputs,
                        schema: u.schema.clone(),
                        alias: u.alias.clone(),
                    });

                    // Add Union to HashMap
                    new_plan.insert(counter, union_plan);
                }
                _ => {
                    warn!("Skipping optimizer rule 'FilterColumnsPostJoin'");
                    return Ok(plan.clone());
                }
            }
            counter += 1;
            break;
        } else {
            match current_plan {
                LogicalPlan::Projection(ref p) => {
                    new_plan.insert(counter, current_plan.clone());
                    previous_projection = true;

                    // Reset HashSet with projected columns
                    post_join_columns = HashSet::new();
                    insert_post_join_columns(&mut post_join_columns, &p.expr);
                }
                _ => {
                    // Is not a Join, so just add LogicalPlan to HashMap
                    new_plan.insert(counter, current_plan.clone());
                    previous_projection = false;

                    let current_columns = &current_plan.expressions();
                    insert_post_join_columns(&mut post_join_columns, current_columns);
                }
            }
            // Move on to next step
            current_plan = current_plan.inputs()[0].clone();
            counter += 1;
        }
    }

    if join_flag {
        counter -= 1;
        // Organize HashMap into LogicalPlan to return
        let mut previous_step = new_plan.get(&counter).unwrap();
        let mut next_step;
        let mut return_plan = plan.clone();

        for key in (0..counter).rev() {
            next_step = new_plan.get(&key).unwrap();
            match next_step {
                LogicalPlan::Projection(p) => {
                    return_plan = LogicalPlan::Projection(Projection {
                        expr: p.expr.clone(),
                        input: Arc::new(previous_step.clone()),
                        schema: p.schema.clone(),
                        alias: p.alias.clone(),
                    });
                }
                LogicalPlan::SubqueryAlias(s) => {
                    return_plan = LogicalPlan::SubqueryAlias(SubqueryAlias {
                        input: Arc::new(previous_step.clone()),
                        alias: s.alias.clone(),
                        schema: s.schema.clone(),
                    });
                }
                LogicalPlan::Filter(f) => {
                    return_plan = LogicalPlan::Filter(
                        Filter::try_new(f.predicate().clone(), Arc::new(previous_step.clone()))
                            .unwrap(),
                    );
                }
                LogicalPlan::Window(w) => {
                    return_plan = LogicalPlan::Window(Window {
                        input: Arc::new(previous_step.clone()),
                        window_expr: w.window_expr.clone(),
                        schema: w.schema.clone(),
                    });
                }
                LogicalPlan::Repartition(r) => {
                    return_plan = LogicalPlan::Repartition(Repartition {
                        input: Arc::new(previous_step.clone()),
                        partitioning_scheme: r.partitioning_scheme.clone(),
                    });
                }
                LogicalPlan::CreateMemoryTable(c) => {
                    return_plan = LogicalPlan::CreateMemoryTable(CreateMemoryTable {
                        name: c.name.clone(),
                        input: Arc::new(previous_step.clone()),
                        if_not_exists: c.if_not_exists,
                        or_replace: c.or_replace,
                    });
                }
                LogicalPlan::CreateView(c) => {
                    return_plan = LogicalPlan::CreateView(CreateView {
                        name: c.name.clone(),
                        input: Arc::new(previous_step.clone()),
                        or_replace: c.or_replace,
                        definition: c.definition.clone(),
                    });
                }
                LogicalPlan::Explain(e) => {
                    return_plan = LogicalPlan::Explain(Explain {
                        verbose: e.verbose,
                        plan: Arc::new(previous_step.clone()),
                        stringified_plans: e.stringified_plans.clone(),
                        schema: e.schema.clone(),
                    });
                }
                LogicalPlan::Analyze(a) => {
                    return_plan = LogicalPlan::Analyze(Analyze {
                        verbose: a.verbose,
                        input: Arc::new(previous_step.clone()),
                        schema: a.schema.clone(),
                    });
                }
                LogicalPlan::Limit(l) => {
                    return_plan = LogicalPlan::Limit(Limit {
                        skip: l.skip,
                        fetch: l.fetch,
                        input: Arc::new(previous_step.clone()),
                    });
                }
                LogicalPlan::Distinct(_) => {
                    return_plan = LogicalPlan::Distinct(Distinct {
                        input: Arc::new(previous_step.clone()),
                    });
                }
                LogicalPlan::Aggregate(a) => {
                    return_plan = LogicalPlan::Aggregate(Aggregate {
                        input: Arc::new(previous_step.clone()),
                        group_expr: a.group_expr.clone(),
                        aggr_expr: a.aggr_expr.clone(),
                        schema: a.schema.clone(),
                    });
                }
                LogicalPlan::Sort(s) => {
                    return_plan = LogicalPlan::Sort(Sort {
                        expr: s.expr.clone(),
                        input: Arc::new(previous_step.clone()),
                        fetch: s.fetch,
                    });
                }
                LogicalPlan::Subquery(_) => {
                    return_plan = LogicalPlan::Subquery(Subquery {
                        subquery: Arc::new(previous_step.clone()),
                    });
                }
                _ => {
                    warn!("Skipping optimizer rule 'FilterColumnsPostJoin'");
                    return Ok(plan.clone());
                }
            }
            previous_step = &return_plan;
        }

        Ok(return_plan)
    } else {
        Ok(plan.clone())
    }
}

fn filter_post_join_columns(post_join_columns: &HashSet<Expr>, table_scan: Option<LogicalPlan>) -> HashSet<Expr> {
    match table_scan {
        Some(LogicalPlan::TableScan(t)) => {
            let mut result = HashSet::new();
            let table_name = t.table_name + ".";
            for column in post_join_columns {
                match column {
                    Expr::Column(c) => {
                        let column_name = c.flat_name();
                        if !column_name.contains(&table_name) {
                            result.insert(column.clone());
                        } else {
                        }
                    }
                    _ => (),
                }
            }
            result
        }
        _ => post_join_columns.clone(),
    }
}

fn insert_post_join_columns(post_join_columns: &mut HashSet<Expr>, inserted_columns: &Vec<Expr>) {
    for column in inserted_columns {
        if let Some(exprs) = get_column_name(column) {
            for expr in exprs {
                post_join_columns.insert(expr);
            }
        }
    }
}

fn get_column_name(column: &Expr) -> Option<Vec<Expr>> {
    // TODO: Can we use to_columns() or expr_to_columns() here?
    match column {
        Expr::Column(c) => {
            let mut column_string = c.flat_name();
            if column_string.contains(')') {
                let start_bytes = column_string.find('(').unwrap_or(0) + 1;
                let end_bytes = column_string.find(')').unwrap_or(0);
                if start_bytes < end_bytes {
                    column_string = column_string[start_bytes..end_bytes].to_string();
                    if column_string.contains('(') {
                        column_string += ")";
                    }
                    Some(vec![Expr::Column(Column::from_qualified_name(
                        &column_string,
                    ))])
                } else {
                    Some(vec![column.clone()])
                }
            } else {
                Some(vec![column.clone()])
            }
        }
        Expr::AggregateFunction { args, filter, .. } => {
            let mut return_vector = vec![];

            for arg in args {
                return_vector = push_column_names(arg, &return_vector);
            }

            for f in filter {
                return_vector = push_column_names(f, &return_vector);
            }

            Some(return_vector)
        }
        Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
            let mut return_vector = vec![];

            return_vector = push_column_names(left, &return_vector);
            return_vector = push_column_names(right, &return_vector);

            Some(return_vector)
        }
        Expr::ScalarFunction { args, .. } => {
            let mut return_vector = vec![];
            for arg in args {
                return_vector = push_column_names(arg, &return_vector);
            }
            Some(return_vector)
        }
        Expr::Sort { expr, .. } => {
            let mut return_vector = vec![];
            return_vector = push_column_names(expr, &return_vector);
            Some(return_vector)
        }
        Expr::Alias(a, _) => {
            let mut return_vector = vec![];
            return_vector = push_column_names(a, &return_vector);
            Some(return_vector)
        }
        Expr::Case(c) => {
            let mut return_vector = vec![];

            let case_expr = &c.expr;
            if let Some(ce) = case_expr {
                return_vector = push_column_names(ce, &return_vector);
            }

            // Vec<(Box<Expr>, Box<Expr>)>
            let when_then_expr = &c.when_then_expr;
            for wte in when_then_expr {
                let wte0 = &wte.0;
                return_vector = push_column_names(wte0, &return_vector);

                let wte1 = &wte.1;
                return_vector = push_column_names(wte1, &return_vector);
            }

            let else_expr = &c.else_expr;
            if let Some(ce) = else_expr {
                return_vector = push_column_names(ce, &return_vector);
            }

            Some(return_vector)
        }
        Expr::IsNull(expr) => {
            let mut return_vector = vec![];
            return_vector = push_column_names(expr, &return_vector);
            Some(return_vector)
        }
        _ => None,
    }
}

fn push_column_names(column: &Expr, vector: &Vec<Expr>) -> Vec<Expr> {
    let mut return_vector = vector.to_owned();
    let exprs = get_column_name(column);
    if let Some(expr) = exprs {
        for e in expr {
            return_vector.push(e);
        }
    }
    return_vector
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::{
        col,
        logical_plan::{builder::LogicalTableSource, JoinType, LogicalPlanBuilder},
        sum,
    };

    use super::*;

    /// Optimize with just the filter_columns_post_join rule
    fn optimized_plan_eq(plan: &LogicalPlan, expected1: &str, expected2: &str) -> bool {
        let rule = FilterColumnsPostJoin::new();
        let optimized_plan = rule
            .optimize(plan, &mut OptimizerConfig::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{}", optimized_plan.display_indent());

        if formatted_plan == expected1 || formatted_plan == expected2 {
            true
        } else {
            false
        }
    }

    #[test]
    fn test_single_join() -> Result<()> {
        // Projection: SUM(df.a), df2.b
        //   Aggregate: groupBy=[[df2.b]], aggr=[[SUM(df.a)]]
        //     Inner Join: df.c = df2.c
        //       TableScan: df
        //       TableScan: df2
        let plan = LogicalPlanBuilder::from(test_table_scan("df", "a"))
            .join(
                &LogicalPlanBuilder::from(test_table_scan("df2", "b")).build()?,
                JoinType::Inner,
                (vec!["c"], vec!["c"]),
                None,
            )?
            .aggregate(vec![col("df2.b")], vec![sum(col("df.a"))])?
            .project(vec![sum(col("df.a")), col("df2.b")])?
            .build()?;

        let expected1 = "Projection: SUM(df.a), df2.b\
        \n  Aggregate: groupBy=[[df2.b]], aggr=[[SUM(df.a)]]\
        \n    Projection: df.a, df2.b\
        \n      Inner Join: df.c = df2.c\
        \n        TableScan: df\
        \n        TableScan: df2";

        let expected2 = "Projection: SUM(df.a), df2.b\
        \n  Aggregate: groupBy=[[df2.b]], aggr=[[SUM(df.a)]]\
        \n    Projection: df2.b, df.a\
        \n      Inner Join: df.c = df2.c\
        \n        TableScan: df\
        \n        TableScan: df2";

        assert_eq!(optimized_plan_eq(&plan, expected1, expected2), true);

        Ok(())
    }

    /// Create a LogicalPlanBuilder representing a scan of a table with the provided name and schema.
    /// This is mostly used for testing and documentation.
    pub fn table_scan(
        name: Option<&str>,
        table_schema: &Schema,
        projection: Option<Vec<usize>>,
    ) -> Result<LogicalPlanBuilder> {
        let tbl_schema = Arc::new(table_schema.clone());
        let table_source = Arc::new(LogicalTableSource::new(tbl_schema));
        LogicalPlanBuilder::scan(name.unwrap_or("test"), table_source, projection)
    }

    fn test_table_scan(table_name: &str, column_name: &str) -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new(column_name, DataType::UInt32, false),
            Field::new("c", DataType::UInt32, false),
        ]);
        table_scan(Some(table_name), &schema, None)
            .expect("creating scan")
            .build()
            .expect("building plan")
    }
}
