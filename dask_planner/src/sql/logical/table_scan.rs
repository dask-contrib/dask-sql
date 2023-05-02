use std::sync::Arc;

use datafusion_common::DFSchema;
use datafusion_expr::{logical_plan::TableScan, Expr, LogicalPlan};
use pyo3::prelude::*;

use crate::{
    error::DaskPlannerError,
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
    // Certain Expr(s) do not have supporting logic in pyarrow for IO filtering
    // at read time. Those Expr(s) cannot be ignored however. This field stores
    // those Expr(s) so that they can be used on the Python side to create
    // Dask operations that handle that filtering as an extra task in the graph.
    #[pyo3(get)]
    pub io_unfilterable_exprs: Vec<PyExpr>,
    // Expr(s) that can have their filtering logic performed in the pyarrow IO logic
    // are stored here in a DNF format that is expected by pyarrow.
    #[pyo3(get)]
    pub filtered_exprs: Vec<(String, String, Vec<String>)>,
}

impl PyTableScan {
    /// Ensures that a valid Expr variant type is present
    fn _valid_expr_type(expr: &[Expr]) -> bool {
        expr.iter()
            .all(|f| matches!(f, Expr::Column(_) | Expr::Literal(_)))
    }

    /// Transform the singular Expr instance into its DNF form serialized in a Vec instance. Possibly recursively expanding
    /// it as well if needed.
    pub fn _expand_dnf_filter(
        filter: &Expr,
    ) -> Result<Vec<(String, String, Vec<String>)>, DaskPlannerError> {
        let mut filter_tuple: Vec<(String, String, Vec<String>)> = Vec::new();

        match filter {
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                // Only handle simple Expr(s) for InList operations for now
                if PyTableScan::_valid_expr_type(list) {
                    let il: Vec<String> = list.iter().map(|f| f.canonical_name()).collect();
                    let op = if *negated { "not in" } else { "in" };

                    // While ANSI SQL would not allow for anything other than a Column or Literal
                    // value in this "identifying" `expr` we explicitly check that here just to be sure.
                    // IF it is something else it is returned to Dask to handle
                    if let Expr::Column(_) | Expr::Literal(_) = **expr {
                        filter_tuple.push((expr.canonical_name(), op.to_string(), il));
                        Ok(filter_tuple)
                    } else {
                        let er = DaskPlannerError::InvalidIOFilter(format!(
                            "Invalid InList Expr type `{}`. using in Dask instead",
                            filter
                        ));
                        Err::<Vec<(String, String, Vec<String>)>, DaskPlannerError>(er)
                    }
                } else {
                    let er = DaskPlannerError::InvalidIOFilter(format!(
                        "Invalid identifying column Expr instance `{}`. using in Dask instead",
                        filter
                    ));
                    Err::<Vec<(String, String, Vec<String>)>, DaskPlannerError>(er)
                }
            }
            _ => {
                let er = DaskPlannerError::InvalidIOFilter(format!(
                    "Unable to apply filter: `{}` to IO reader, using in Dask instead",
                    filter
                ));
                Err::<Vec<(String, String, Vec<String>)>, DaskPlannerError>(er)
            }
        }
    }

    /// Consume the `TableScan` filters (Expr(s)) and convert them into a PyArrow understandable
    /// DNF format that can be directly passed to PyArrow IO readers for Predicate Pushdown. Expr(s)
    /// that cannot be converted to correlating PyArrow IO calls will be returned as is and can be
    /// used in the Python logic to form Dask tasks for the graph to do computational filtering.
    pub fn _expand_dnf_filters(input: &Arc<LogicalPlan>, filters: &[Expr]) -> PyFilteredResult {
        let mut filtered_exprs: Vec<(String, String, Vec<String>)> = Vec::new();
        let mut unfiltered_exprs: Vec<PyExpr> = Vec::new();

        filters
            .iter()
            .for_each(|f| match PyTableScan::_expand_dnf_filter(f) {
                Ok(mut expanded_dnf_filter) => filtered_exprs.append(&mut expanded_dnf_filter),
                Err(_e) => {
                    unfiltered_exprs.push(PyExpr::from(f.clone(), Some(vec![input.clone()])))
                }
            });

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

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
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
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{Result, TableReference};
    use datafusion_expr::{col, in_list, lit, logical_plan::table_scan, Expr};

    use super::PyTableScan;

    #[test]
    fn dnf_inlist() -> Result<()> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        // Dummy logical plan
        let plan = Arc::new(
            table_scan(TableReference::none(), &schema, None)
                .unwrap()
                .filter(col("id").eq(Expr::Placeholder {
                    id: "".into(),
                    data_type: Some(DataType::Int32),
                }))
                .unwrap()
                .build()
                .unwrap(),
        );

        let il = in_list(col("id"), vec![lit(1), lit(2), lit(3)], false);
        let results = PyTableScan::_expand_dnf_filters(&plan, &vec![il]);

        assert_eq!(results.io_unfilterable_exprs.len(), 0);
        assert_eq!(results.filtered_exprs.len(), 1);
        assert_eq!(results.filtered_exprs[0].0, "id");
        assert_eq!(results.filtered_exprs[0].1, "in");
        assert_eq!(results.filtered_exprs[0].2[0], "Int32(1)");
        assert_eq!(results.filtered_exprs[0].2[1], "Int32(2)");
        assert_eq!(results.filtered_exprs[0].2[2], "Int32(3)");

        Ok(())
    }
}
