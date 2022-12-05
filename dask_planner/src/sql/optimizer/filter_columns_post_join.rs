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

use std::{collections::{HashMap, HashSet}, sync::Arc};

use datafusion_common::{Column, Result};
use datafusion_expr::{
    logical_plan::{Join, LogicalPlan, Projection},
    Expr,
};
use datafusion_optimizer::{utils, OptimizerConfig, OptimizerRule};

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
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
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

        // Second: Iterate through LogicalPlan with &plan.inputs()
        let mut counter = 0;
        let mut current_step = plan.inputs()[0];  // &Vec<&LogicalPlan>[0]
        let mut join_flag = false;

        loop {
            match current_step {
                LogicalPlan::Join(Join {
                    ref schema,
                    ..
                }) => {
                    join_flag = true;

                    // Remove duplicates from HashSet
                    post_join_columns = post_join_columns.iter().cloned().collect();
                    // Convert HashSet to Vector
                    let projection_columns: Vec<Expr> = post_join_columns.clone().into_iter().collect();
                    // Create a Projection with the columns from the HashSet
                    let projection_step = LogicalPlan::Projection(Projection {
                        expr: projection_columns,
                        input: Arc::new(current_step.clone()),
                        schema: schema.clone(),
                        alias: None,
                    });

                    // TODO: Should we clear the HashSet here?

                    // Add Projection to HashMap
                    new_plan.insert(counter, projection_step);
                    // Add Join to HashMap
                    counter = counter + 1;
                    new_plan.insert(counter, current_step.clone());
                }
                _ => {
                    // Is not a Join, so just add LogicalPlan to HashMap
                    new_plan.insert(counter, current_step.clone());
                }
            }

            let current_columns = &current_step.expressions();
            for column in current_columns {
                // TODO: Check that we are properly parsing all columns
                if let Some(expr) = get_column_name(column) {
                    post_join_columns.insert(expr);
                }
            }

            if current_step.inputs().is_empty() {
                break
            } else {
                current_step = current_step.inputs()[0];
                counter = counter + 1;
            }
        }

        if join_flag {
            // TODO: Organize HashMap into LogicalPlan to return
            let mut next_step;
            for key in (0..counter).rev() {
                // println!("{:?}", &new_plan.get(&key));  // TODO: Remove
                next_step = new_plan.get(&key);
            }
            Ok(plan.clone())  // TODO: Remove
        } else {
            Ok(plan.clone())
        }
    }

    fn name(&self) -> &str {
        "filter_columns_post_join"
    }
}

fn get_column_name(column: &Expr) -> Option<Expr> {
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
