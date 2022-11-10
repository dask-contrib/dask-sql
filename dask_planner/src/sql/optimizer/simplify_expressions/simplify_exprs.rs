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

//! Simplify expressions optimizer rule and implementation

use datafusion_common::Result;
use datafusion_expr::{logical_plan::LogicalPlan, utils::from_plan};
use datafusion_optimizer::{simplify_expressions::SimplifyContext, OptimizerConfig, OptimizerRule};
use datafusion_physical_expr::execution_props::ExecutionProps;

use super::ExprSimplifier;

/// Optimizer Pass that simplifies [`LogicalPlan`]s by rewriting
/// [`Expr`]`s evaluating constants and applying algebraic
/// simplifications
///
/// # Introduction
/// It uses boolean algebra laws to simplify or reduce the number of terms in expressions.
///
/// # Example:
/// `Filter: b > 2 AND b > 2`
/// is optimized to
/// `Filter: b > 2`
///
#[derive(Default)]
pub struct SimplifyExpressions {}

impl OptimizerRule for SimplifyExpressions {
    fn name(&self) -> &str {
        "simplify_expressions"
    }

    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        let mut execution_props = ExecutionProps::new();
        execution_props.query_execution_start_time = optimizer_config.query_execution_start_time();
        Self::optimize_internal(plan, &execution_props)
    }
}

impl SimplifyExpressions {
    fn optimize_internal(
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        // We need to pass down the all schemas within the plan tree to `optimize_expr` in order to
        // to evaluate expression types. For example, a projection plan's schema will only include
        // projected columns. With just the projected schema, it's not possible to infer types for
        // expressions that references non-projected columns within the same project plan or its
        // children plans.
        let info = plan
            .all_schemas()
            .into_iter()
            .fold(SimplifyContext::new(execution_props), |context, schema| {
                context.with_schema(schema.clone())
            });

        let simplifier = ExprSimplifier::new(info);

        let new_inputs = plan
            .inputs()
            .iter()
            .map(|input| Self::optimize_internal(input, execution_props))
            .collect::<Result<Vec<_>>>()?;

        let expr = plan
            .expressions()
            .into_iter()
            .map(|e| {
                // We need to keep original expression name, if any.
                // Constant folding should not change expression name.
                let name = &e.display_name();

                // Apply the actual simplification logic
                let new_e = simplifier.simplify(e)?;

                let new_name = &new_e.display_name();

                if let (Ok(expr_name), Ok(new_expr_name)) = (name, new_name) {
                    if expr_name != new_expr_name {
                        Ok(new_e.alias(expr_name))
                    } else {
                        Ok(new_e)
                    }
                } else {
                    Ok(new_e)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        from_plan(plan, &expr, &new_inputs)
    }
}

impl SimplifyExpressions {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}
