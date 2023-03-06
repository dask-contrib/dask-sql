use std::sync::Arc;

use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{logical_plan::TableScan, Expr, LogicalPlan};
use pyo3::prelude::*;

use crate::{
    error::{DaskPlannerError, Result},
    expression::{py_expr_list, PyExpr},
    sql::exceptions::py_type_err,
};

#[pyclass(name = "TableScan", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct PyTableScan {
    pub(crate) table_scan: TableScan,
    input: Arc<LogicalPlan>,
}

#[pyclass(name = "FilteredResult", module = "dask_planner", subclass)]
#[derive(Debug, Clone)]
pub struct PyFilteredResult {
    // Exprs that cannot be successfully passed down to the IO layer for filtering and must still be filtered using Dask operations
    #[pyo3(get)]
    pub io_unfilterable_exprs: Vec<PyExpr>,
    #[pyo3(get)]
    pub filtered_exprs: Vec<(String, String, String)>,
}

impl PyTableScan {
    /// Transform the singular Expr instance into its DNF form serialized in a Vec instance. Possibly recursively expanding
    /// it as well if needed.
    ///
    /// Ex: BinaryExpr("column_name", Operator::Eq, "something") -> vec!["column_name", "=", "something"]
    /// Ex: Expr::Column("column_name") -> vec!["column_name"]
    /// Ex: Expr::Literal(Utf-8("something")) -> vec!["something"]
    pub fn _expand_dnf_filter(filter: &Expr) -> Result<Vec<(String, String, String)>> {
        let mut filter_tuple: Vec<(String, String, String)> = Vec::new();

        match filter {
            Expr::BinaryExpr(binary_expr) => {
                println!(
                    "!!!BinaryExpr -> Left: {:?}, Operator: {:?}, Right: {:?}",
                    binary_expr.left, binary_expr.op, binary_expr.right
                );

                // Since Tuples are immutable in Rust we need a datastructure to temporaily hold the Tuples values until all have been parsed
                let mut tmp_vals: Vec<String> = Vec::new();

                // Push left Expr string value or combo of expanded left values
                match &*binary_expr.left {
                    Expr::BinaryExpr(binary_expr) => {
                        filter_tuple.append(
                            &mut PyTableScan::_expand_dnf_filter(&Expr::BinaryExpr(
                                binary_expr.clone(),
                            ))
                            .unwrap(),
                        );
                    }
                    _ => {
                        let str = binary_expr.left.to_string();
                        if str.contains('.') {
                            tmp_vals.push(str.split('.').nth(1).unwrap().to_string());
                        } else {
                            tmp_vals.push(str);
                        }
                    }
                }

                // Handle the operator here. This controls if the format is conjunctive or disjuntive
                tmp_vals.push(binary_expr.op.to_string());

                match &*binary_expr.right {
                    Expr::BinaryExpr(binary_expr) => {
                        filter_tuple.append(
                            &mut PyTableScan::_expand_dnf_filter(&Expr::BinaryExpr(
                                binary_expr.clone(),
                            ))
                            .unwrap(),
                        );
                    }
                    _ => match &*binary_expr.right {
                        Expr::Literal(scalar_value) => match &scalar_value {
                            ScalarValue::Utf8(value) => {
                                let val = value.as_ref().unwrap();
                                if val.contains('.') {
                                    tmp_vals.push(val.split('.').nth(1).unwrap().to_string());
                                } else {
                                    tmp_vals.push(val.clone());
                                }
                            }
                            ScalarValue::TimestampNanosecond(_val, _an_option) => {
                                // // Need to encode the value as a String to return to Python, Python side will then convert
                                // // value back to a integer
                                // let mut val_builder = "TimestampNanosecond(".to_string();
                                // val_builder.push_str(val.unwrap().to_string().as_str());
                                // val_builder.push(')');
                                // tmp_vals.push(val_builder);
                                // let er =
                                //     DaskPlannerError::InvalidIOFilter(scalar_value.to_string());
                                // Err::<Vec<(String, String, String)>, DaskPlannerError>(er)

                                // SIMPLY DO NOT PUSH THE VALUES HERE AND IT WILL CAUSE THE ERROR TO BE PROPAGATED
                            }
                            _ => tmp_vals.push(scalar_value.to_string()),
                        },
                        _ => panic!("hit this!"),
                    },
                }

                if tmp_vals.len() == 3 {
                    filter_tuple.push((
                        tmp_vals[0].clone(),
                        tmp_vals[1].clone(),
                        tmp_vals[2].clone(),
                    ));

                    Ok(filter_tuple)
                } else {
                    println!(
                        "Wonder why tmp_vals doesn't equal 3?? {:?}, Value: {:?}",
                        tmp_vals.len(),
                        tmp_vals[0]
                    );
                    let er = DaskPlannerError::InvalidIOFilter(format!(
                        "Wonder why tmp_vals doesn't equal 3?? {:?}, Value: {:?}",
                        tmp_vals.len(),
                        tmp_vals[0]
                    ));
                    Err::<Vec<(String, String, String)>, DaskPlannerError>(er)
                }
            }
            _ => {
                println!(
                    "Unable to apply filter: `{}` to IO reader, using in Dask instead",
                    filter
                );
                let er = DaskPlannerError::InvalidIOFilter(format!(
                    "Unable to apply filter: `{}` to IO reader, using in Dask instead",
                    filter
                ));
                Err::<Vec<(String, String, String)>, DaskPlannerError>(er)
            }
        }

        // Ok(filter_tuple)
    }

    pub fn _expand_dnf_filters(input: &Arc<LogicalPlan>, filters: &Vec<Expr>) -> PyFilteredResult {
        // 1. Loop through all of the TableScan filters (Expr(s))
        let mut filtered_exprs: Vec<(String, String, String)> = Vec::new();
        let mut unfiltered_exprs: Vec<PyExpr> = Vec::new();
        for filter in filters {
            match PyTableScan::_expand_dnf_filter(filter) {
                Ok(mut expanded_dnf_filter) => filtered_exprs.append(&mut expanded_dnf_filter),
                Err(_e) => {
                    unfiltered_exprs.push(PyExpr::from(filter.clone(), Some(vec![input.clone()])))
                }
            }
        }

        PyFilteredResult {
            io_unfilterable_exprs: unfiltered_exprs,
            filtered_exprs,
        }
    }
}

#[pymethods]
impl PyTableScan {
    #[pyo3(name = "getTableScanProjects")]
    fn scan_projects(&mut self) -> PyResult<Vec<String>> {
        match &self.table_scan.projection {
            Some(indices) => {
                let schema = self.table_scan.source.schema();
                Ok(indices
                    .iter()
                    .map(|i| schema.field(*i).name().to_string())
                    .collect())
            }
            None => Ok(vec![]),
        }
    }

    /// If the 'TableScan' contains columns that should be projected during the
    /// read return True, otherwise return False
    #[pyo3(name = "containsProjections")]
    fn contains_projections(&self) -> bool {
        self.table_scan.projection.is_some()
    }

    #[pyo3(name = "getFilters")]
    fn scan_filters(&self) -> PyResult<Vec<PyExpr>> {
        py_expr_list(&self.input, &self.table_scan.filters)
    }

    #[pyo3(name = "getDNFFilters")]
    fn dnf_io_filters(&self) -> PyResult<PyFilteredResult> {
        let results = PyTableScan::_expand_dnf_filters(&self.input, &self.table_scan.filters);
        Ok(results)
    }
}

impl TryFrom<LogicalPlan> for PyTableScan {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> std::result::Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::TableScan(table_scan) => {
                // Create an input logical plan that's identical to the table scan with schema from the table source
                let mut input = table_scan.clone();
                input.projected_schema = DFSchema::try_from_qualified_schema(
                    &table_scan.table_name,
                    &table_scan.source.schema(),
                )
                .map_or(input.projected_schema, Arc::new);

                Ok(PyTableScan {
                    table_scan,
                    input: Arc::new(LogicalPlan::TableScan(input)),
                })
            }
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::expr_fn::{binary_expr, col};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{Expr, Operator};

    use crate::sql::logical::table_scan::PyTableScan;

    #[test]
    pub fn expand_binary_exprs_dnf_filters() {
        let jewlery = binary_expr(
            col("item.i_category"),
            Operator::Eq,
            Expr::Literal(ScalarValue::new_utf8("Jewelry")),
        );
        let women = binary_expr(
            col("item.i_category"),
            Operator::Eq,
            Expr::Literal(ScalarValue::new_utf8("Women")),
        );
        let music = binary_expr(
            col("item.i_category"),
            Operator::Eq,
            Expr::Literal(ScalarValue::new_utf8("Music")),
        );

        let full_expr = binary_expr(jewlery, Operator::Or, women);
        let full_expr = binary_expr(full_expr, Operator::Or, music);
        println!("BinaryExpr: {:?}", full_expr);

        let filters = vec![full_expr];

        let result = PyTableScan::_expand_dnf_filters(&filters);
        println!("Result: {:?}", result);
    }
}
