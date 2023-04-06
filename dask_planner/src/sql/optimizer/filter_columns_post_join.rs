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
    CreateMemoryTable,
    CreateView,
    Distinct,
    Explain,
    Expr,
    Filter,
    JoinType,
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
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for FilterColumnsPostJoin {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _optimizer_config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        // Store info about all columns in all schemas
        let all_schemas = &plan.all_schemas();
        Ok(Some(optimize_top_down(plan, all_schemas, HashSet::new())?))
    }

    fn name(&self) -> &str {
        "filter_columns_post_join"
    }
}

fn optimize_top_down(
    plan: &LogicalPlan,
    schemas: &Vec<&Arc<DFSchema>>,
    projected_columns: HashSet<Expr>,
) -> Result<LogicalPlan> {
    let mut post_join_columns: HashSet<Expr> = projected_columns;

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
                    if j.join_type == JoinType::LeftSemi || j.join_type == JoinType::RightSemi {
                        should_project = false;
                    }

                    // Check so that we don't build a stack of projections
                    if !previous_projection && should_project {
                        // Remove un-projectable columns
                        post_join_columns =
                            filter_post_join_columns(&post_join_columns, j.schema.clone());
                        // Remove duplicates from HashSet
                        post_join_columns = post_join_columns.iter().cloned().collect();
                        // Convert HashSet to Vector
                        let projection_columns: Vec<Expr> =
                            post_join_columns.clone().into_iter().collect();

                        // Prepare schema
                        let mut projection_schema_fields = vec![];
                        for column in &post_join_columns {
                            for dfschema in schemas {
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
                        let projection_plan =
                            LogicalPlan::Projection(Projection::try_new_with_schema(
                                projection_columns,
                                Arc::new(current_plan.clone()),
                                Arc::new(projection_schema?),
                            )?);

                        // Add Projection to HashMap
                        new_plan.insert(counter, projection_plan);
                        counter += 1;
                    }

                    insert_post_join_columns(&mut post_join_columns, current_columns);

                    // Recurse on left and right inputs of Join
                    let left_join_plan = optimize_top_down(
                        &j.left,
                        &j.left.all_schemas(),
                        post_join_columns.clone(),
                    )?;
                    let right_join_plan = optimize_top_down(
                        &j.right,
                        &j.right.all_schemas(),
                        post_join_columns.clone(),
                    )?;
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
                    let left_crossjoin_plan = optimize_top_down(
                        &c.left,
                        &c.left.all_schemas(),
                        post_join_columns.clone(),
                    )?;
                    let right_crossjoin_plan = optimize_top_down(
                        &c.right,
                        &c.right.all_schemas(),
                        post_join_columns.clone(),
                    )?;
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
                        let new_input = optimize_top_down(
                            input,
                            &input.all_schemas(),
                            post_join_columns.clone(),
                        );
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
                    return_plan = LogicalPlan::Projection(Projection::try_new_with_schema(
                        p.expr.clone(),
                        Arc::new(previous_step.clone()),
                        p.schema.clone(),
                    )?);
                }
                LogicalPlan::SubqueryAlias(s) => {
                    return_plan = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                        previous_step.clone(),
                        s.alias.clone(),
                    )?);
                }
                LogicalPlan::Filter(f) => {
                    return_plan = LogicalPlan::Filter(Filter::try_new(
                        f.predicate.clone(),
                        Arc::new(previous_step.clone()),
                    )?);
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
                        logical_optimization_succeeded: false, // While Dask-SQL does not use the DataFusion Physical Planner we should set this value to False to guard any 3rd party dependency using Dask-SQL from assuming the plan is safe to use at this point.
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
                    return_plan = LogicalPlan::Aggregate(Aggregate::try_new_with_schema(
                        Arc::new(previous_step.clone()),
                        a.group_expr.clone(),
                        a.aggr_expr.clone(),
                        a.schema.clone(),
                    )?);
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

fn filter_post_join_columns(
    post_join_columns: &HashSet<Expr>,
    dfschema: Arc<DFSchema>,
) -> HashSet<Expr> {
    let mut result = HashSet::new();

    // Check which tables/aliases are needed at this point in the plan
    let mut valid_qualifiers: Vec<&str> = vec![];
    let dfschema_fields = dfschema.fields();
    for field in dfschema_fields {
        let qualifier = field.qualifier();
        if let Some(q) = qualifier {
            valid_qualifiers.push(q);
        }
    }

    // Remove a column if the table/alias has not been read/used
    for column in post_join_columns {
        if let Expr::Column(c) = column {
            let column_qualifier = &c.relation;
            if let Some(q) = column_qualifier {
                if valid_qualifiers.contains(&q.as_str()) {
                    result.insert(column.clone());
                }
            } else {
                result.insert(column.clone());
            }
        }
    }
    result
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
    // Returns a Result<HashSet>
    let hs = column.to_columns().unwrap();

    let mut result = vec![];
    for col in hs {
        let mut column_relation = col.relation.unwrap_or_default();
        if !column_relation.is_empty() {
            column_relation += ".";
        }
        // Grab the column name and check that it's not a function
        let column_string = col.name;
        if !column_string.contains(')') {
            let column_result = column_relation + &column_string;
            result.push(Expr::Column(Column::from_qualified_name(&column_result)));
        }
    }

    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::{
        col,
        logical_plan::{builder::LogicalTableSource, JoinType, LogicalPlanBuilder},
        sum,
    };
    use datafusion_optimizer::OptimizerContext;

    use super::*;

    /// Optimize with just the filter_columns_post_join rule
    fn optimized_plan_eq(plan: &LogicalPlan, expected1: &str, expected2: &str) -> bool {
        let rule = FilterColumnsPostJoin::new();
        let optimized_plan = rule
            .try_optimize(plan, &OptimizerContext::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{}", optimized_plan.unwrap().display_indent());

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
                LogicalPlanBuilder::from(test_table_scan("df2", "b")).build()?,
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
