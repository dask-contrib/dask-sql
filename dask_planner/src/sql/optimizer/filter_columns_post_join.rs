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
    logical_plan::{Analyze, LogicalPlan, Projection},
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
        // First: Grab projected columns with &plan.expressions() and add to HashSet
        let mut post_join_columns: HashSet<Expr> = HashSet::new();
        let projected_columns = &plan.expressions();
        for column in projected_columns {
            if let Some(exprs) = get_column_name(column) {
                for expr in exprs {
                    post_join_columns.insert(expr);
                }
            }
        }
        // For storing the steps of the LogicalPlan,
        // with an i32 key to keep track of the order
        let mut new_plan: HashMap<i32, LogicalPlan> = HashMap::new();
        new_plan.insert(0, plan.clone());
        // Store info about all columns in all schemas
        let all_schemas = &plan.all_schemas();

        // Second: Iterate through LogicalPlan with &plan.inputs()
        let mut counter = 1;
        let mut current_step = plan.inputs();
        let mut current_plan;
        let mut join_flag = false;

        loop {
            if current_step.is_empty() {
                break;
            } else if current_step.len() > 1 {
                // TODO: Handle a Join, CrossJoin, or Union
                break;
            }
            current_plan = current_step[0];
            match current_plan {
                LogicalPlan::Join(_) => {
                    join_flag = true;

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
                    counter += 1;
                    new_plan.insert(counter, current_plan.clone());
                }
                LogicalPlan::Projection(_) => {
                    // TODO: Check so that we don't build a stack of projections
                    new_plan.insert(counter, current_plan.clone());
                }
                _ => {
                    // Is not a Join, so just add LogicalPlan to HashMap
                    new_plan.insert(counter, current_plan.clone());
                }
            }

            // TODO: Revisit to_columns() or expr_to_columns()
            let current_columns = &current_plan.expressions();
            for column in current_columns {
                if let Some(exprs) = get_column_name(column) {
                    for expr in exprs {
                        post_join_columns.insert(expr);
                    }
                }
            }

            current_step = current_plan.inputs();
            counter += 1;
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

    fn name(&self) -> &str {
        "filter_columns_post_join"
    }
}

fn get_column_name(column: &Expr) -> Option<Vec<Expr>> {
    // TODO: Make more robust
    match column {
        Expr::Column(c) => {
            let mut column_string = c.flat_name();
            if column_string.contains(')') {
                let start_bytes = column_string.find('(').unwrap_or(0) + 1;
                let end_bytes = column_string.find(')').unwrap_or(0);
                if start_bytes < end_bytes {
                    column_string = column_string[start_bytes..end_bytes].to_string();
                    Some(vec![Expr::Column(Column::from_qualified_name(&column_string))])
                } else {
                    Some(vec![column.clone()])
                }
            } else {
                Some(vec![column.clone()])
            }
        }
        Expr::AggregateFunction { args, .. } => {
            Some(args.clone())
        }
        /*
        // TODO
        Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
            // Without: 21 failures
            // With: Also 21 failures, with different queries
            Some(vec![*left.clone(), *right.clone()])
        }
        */
        _ => {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    // TODO: Implement
    fn test_single_join() -> Result<()> {
        Ok(())
    }
    // TODO: Do we need this?
    fn test_single_join_with_aliases() -> Result<()> {
        Ok(())
    }
    // TODO: Implement
    fn test_multiple_joins() -> Result<()> {
        Ok(())
    }
    // TODO: Do we need this?
    fn test_multiple_joins_with_aliases() -> Result<()> {
        Ok(())
    }
}
