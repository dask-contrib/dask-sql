// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The FilterNullJoinKeys rule will identify inner joins with equi-join conditions
//! where the join key is nullable on one side and non-nullable on the other side
//! and then insert an `IsNotNull` filter on the nullable side since null values
//! can never match.

use datafusion_common::{Column, DFField, DFSchemaRef};
use datafusion_expr::{and, logical_plan::Filter, logical_plan::JoinType, Expr, LogicalPlan};
use datafusion_optimizer::{utils, OptimizerConfig, OptimizerRule};
use std::sync::Arc;

/// The FilterNullJoinKeys rule will identify inner joins with equi-join conditions
/// where the join key is nullable on one side and non-nullable on the other side
/// and then insert an `IsNotNull` filter on the nullable side since null values
/// can never match.
#[derive(Default)]
pub struct FilterNullJoinKeys {}

impl OptimizerRule for FilterNullJoinKeys {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &OptimizerConfig,
    ) -> datafusion_common::Result<LogicalPlan> {
        match plan {
            LogicalPlan::Join(join) if join.join_type == JoinType::Inner => {
                // recurse down first and optimize inputs
                let mut join = join.clone();
                join.left = Arc::new(self.optimize(&join.left, optimizer_config)?);
                join.right = Arc::new(self.optimize(&join.right, optimizer_config)?);

                let left_schema = join.left.schema();
                let right_schema = join.right.schema();

                let mut left_filters = vec![];
                let mut right_filters = vec![];

                for (l, r) in &join.on {
                    if let Some((left_field, right_field)) =
                        resolve_join_key_pair(left_schema, right_schema, l, r)
                    {
                        if left_field.is_nullable() && !right_field.is_nullable() {
                            left_filters.push(l.clone());
                        } else if !left_field.is_nullable() && right_field.is_nullable() {
                            right_filters.push(r.clone());
                        }
                    }
                }

                if !left_filters.is_empty() {
                    let predicate = create_not_null_predicate(left_filters);
                    join.left = Arc::new(LogicalPlan::Filter(Filter {
                        predicate,
                        input: join.left.clone(),
                    }));
                }
                if !right_filters.is_empty() {
                    let predicate = create_not_null_predicate(right_filters);
                    join.right = Arc::new(LogicalPlan::Filter(Filter {
                        predicate,
                        input: join.right.clone(),
                    }));
                }
                Ok(LogicalPlan::Join(join))
            }
            _ => {
                // Apply the optimization to all inputs of the plan
                utils::optimize_children(self, plan, optimizer_config)
            }
        }
    }

    fn name(&self) -> &str {
        "FilterNullJoinKeys"
    }
}

fn create_not_null_predicate(columns: Vec<Column>) -> Expr {
    let not_null_exprs: Vec<Expr> = columns
        .into_iter()
        .map(|c| Expr::IsNotNull(Box::new(Expr::Column(c))))
        .collect();
    // combine the IsNotNull expressions with AND
    not_null_exprs
        .iter()
        .skip(1)
        .fold(not_null_exprs[0].clone(), |a, b| and(a, b.clone()))
}

fn resolve_join_key_pair(
    left_schema: &DFSchemaRef,
    right_schema: &DFSchemaRef,
    c1: &Column,
    c2: &Column,
) -> Option<(DFField, DFField)> {
    resolve_fields(left_schema, right_schema, c1, c2)
        .or_else(|| resolve_fields(left_schema, right_schema, c2, c1))
}

fn resolve_fields(
    left_schema: &DFSchemaRef,
    right_schema: &DFSchemaRef,
    c1: &Column,
    c2: &Column,
) -> Option<(DFField, DFField)> {
    match (
        left_schema.index_of_column(c1),
        right_schema.index_of_column(c2),
    ) {
        (Ok(left_index), Ok(right_index)) => {
            let left_field = left_schema.field(left_index);
            let right_field = right_schema.field(right_index);
            Some((left_field.clone(), right_field.clone()))
        }
        _ => None,
    }
}
