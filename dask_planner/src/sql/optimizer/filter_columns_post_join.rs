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

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use datafusion_common::{Column, DFSchema, Result};
use datafusion_expr::{
    logical_plan::{Join, LogicalPlan, Projection, Analyze},
    Expr,
    SubqueryAlias, Filter, Window, Repartition, CreateMemoryTable, CreateView, Explain, Limit, Distinct, Aggregate, Sort, Subquery,
};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};

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
        // TODO: Break code into functions where appropriate

        // First: Grab projected columns with &plan.expressions() and add to HashSet
        let mut post_join_columns: HashSet<Expr> = HashSet::new();
        let projected_columns = &plan.expressions();
        for column in projected_columns {
            if let Some(expr) = get_column_name(column) {
                post_join_columns.insert(expr);
            }
        }
        // For storing the steps of the LogicalPlan,
        // with an i32 key to keep track of the order
        let mut new_plan: HashMap<i32, LogicalPlan> = HashMap::new();
        new_plan.insert(0, plan.clone());

        // Second: Iterate through LogicalPlan with &plan.inputs()
        let mut counter = 1;
        let mut current_step = plan.inputs();  // &Vec<&LogicalPlan>
        let mut current_plan;
        let mut join_flag = false;
        let mut previous_projection = false;

        loop {
            if current_step.len() > 1 {
                break;  // TODO: Check logic here
            }
            current_plan = current_step[0];
            match current_plan {
                LogicalPlan::Join(Join { ref schema, .. }) => {
                    join_flag = true;

                    // Remove duplicates from HashSet
                    post_join_columns = post_join_columns.iter().cloned().collect();
                    // Convert HashSet to Vector
                    let projection_columns: Vec<Expr> =
                        post_join_columns.clone().into_iter().collect();
                    // Prepare schema
                    let current_schema_fields = schema.fields().clone();
                    let mut projection_schema_fields = vec![];
                    // TODO: Ensure that all columns are added to schema
                    for column in post_join_columns {
                        for field in &current_schema_fields {
                            let field_expr = Expr::Column(
                                Column::from_qualified_name(&field.qualified_name()),
                            );
                            if column == field_expr {
                                projection_schema_fields.push(field.clone());
                            }
                        }
                    }
                    // TODO: new() is deprecated
                    let projection_schema = DFSchema::new(projection_schema_fields);
                    // Create a Projection with the columns from the HashSet
                    let projection_step = LogicalPlan::Projection(Projection {
                        expr: projection_columns,
                        input: Arc::new(current_plan.clone()),
                        schema: Arc::new(projection_schema.unwrap()),
                        alias: None,
                    });

                    // TODO: Should we clear the HashSet here?

                    // Add Projection to HashMap
                    new_plan.insert(counter, projection_step);
                    // Add Join to HashMap
                    counter = counter + 1;
                    new_plan.insert(counter, current_plan.clone());
                    previous_projection = true;
                    // TODO: Work on logic for multiple Joins
                    break;  // TODO: Remove to allow multiple Joins
                }
                LogicalPlan::Projection(_) => {
                    // TODO: Check so that we don't build a stack of projections
                    if !previous_projection {
                        // Is not a Join, so just add LogicalPlan to HashMap
                        new_plan.insert(counter, current_plan.clone());
                    } else {
                        counter = counter - 1;
                    }
                    previous_projection = true;
                }
                _ => {
                    // Is not a Join, so just add LogicalPlan to HashMap
                    new_plan.insert(counter, current_plan.clone());
                    previous_projection = false;
                }
            }

            let current_columns = &current_plan.expressions();
            for column in current_columns {
                // TODO: Check that we are properly parsing all columns
                if let Some(expr) = get_column_name(column) {
                    post_join_columns.insert(expr);
                }
            }

            if current_plan.inputs().is_empty() {
                break;
            } else {
                current_step = current_plan.inputs();
                counter = counter + 1;
            }
        }

        if join_flag {
            counter = counter - 1;
            // Organize HashMap into LogicalPlan to return
            // TODO: Use a LogicalPlanBuilder instead
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
                    /*
                    // TODO: Use private fields
                    LogicalPlan::Filter(f) => {
                        return_plan = LogicalPlan::Filter(Filter {
                            predicate: f.predicate().clone(),
                            input: Arc::new(previous_step.clone()),
                        });
                    }
                    */
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
                            if_not_exists: c.if_not_exists.clone(),
                            or_replace: c.or_replace.clone(),
                        });
                    }
                    LogicalPlan::CreateView(c) => {
                        return_plan = LogicalPlan::CreateView(CreateView {
                            name: c.name.clone(),
                            input: Arc::new(previous_step.clone()),
                            or_replace: c.or_replace.clone(),
                            definition: c.definition.clone(),
                        });
                    }
                    LogicalPlan::Explain(e) => {
                        return_plan = LogicalPlan::Explain(Explain {
                            verbose: e.verbose.clone(),
                            plan: Arc::new(previous_step.clone()),
                            stringified_plans: e.stringified_plans.clone(),
                            schema: e.schema.clone(),
                        });
                    }
                    LogicalPlan::Analyze(a) => {
                        return_plan = LogicalPlan::Analyze(Analyze {
                            verbose: a.verbose.clone(),
                            input: Arc::new(previous_step.clone()),
                            schema: a.schema.clone(),
                        });
                    }
                    LogicalPlan::Limit(l) => {
                        return_plan = LogicalPlan::Limit(Limit {
                            skip: l.skip.clone(),
                            fetch: l.fetch.clone(),
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
                            fetch: s.fetch.clone(),
                        });
                    }
                    LogicalPlan::Subquery(_) => {
                        return_plan = LogicalPlan::Subquery(Subquery {
                            subquery: Arc::new(previous_step.clone()),
                        });
                    }
                    // LogicalPlan::Join(f) => _,
                    // LogicalPlan::CrossJoin(f) => _,
                    // LogicalPlan::Union(f) => _,
                    _ => return Ok(plan.clone()),
                }
                previous_step = &return_plan;
            }

            Ok(return_plan)
        } else {
            Ok(plan.clone())
        }
    }

    fn name(&self) -> &str {
        "filter_columns_post_join"
    }
}

fn get_column_name(column: &Expr) -> Option<Expr> {
    // TODO: Make more robust
    match column {
        Expr::Column(c) => {
            let mut column_string = c.flat_name();
            if column_string.contains(")") {
                let start_bytes = column_string.find("(").unwrap_or(0) + 1;
                let end_bytes = column_string.find(")").unwrap_or(0);
                column_string = (&column_string[start_bytes..end_bytes]).to_string();
                Some(Expr::Column(Column::from_qualified_name(&column_string)))
            } else {
                Some(column.clone())
            }
        }
        _ => None,
    }
}

// TODO: Add tests
