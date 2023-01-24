use std::sync::Arc;

use datafusion_common::DFSchema;
use datafusion_expr::{Expr, logical_plan::TableScan, LogicalPlan};
use pyo3::prelude::*;

use crate::{
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
        let mut filters: Vec<(String, String, String)> = Vec::new();
        let mut unfiltered: Vec<PyExpr> = Vec::new();
        for filter in &self.table_scan.filters {
            match filter {
                Expr::BinaryExpr(binary_expr) => {
                    let left = binary_expr.left.to_string();
                    let mut left_split = left.split('.');
                    let left = left_split.nth(1);
                    let right = binary_expr.right.to_string();
                    let mut right_split = right.split('.');
                    let right = right_split.nth(0);
                    filters.push((left.unwrap().to_string(), binary_expr.op.to_string(), right.unwrap().to_string()))
                },
                Expr::IsNotNull(inner_expr) => {
                    println!("IS NOT NULL Expr: {:?}", inner_expr);
                    let fqtn = inner_expr.to_string();
                    let mut col_split = fqtn.split('.');
                    let col = col_split.nth(1);
                    filters.push((col.unwrap().to_string(), "!=".to_string(), "np.nan".to_string()))
                },
                _ => {
                    println!("Unable to apply filter: `{}` to IO reader, using in Dask instead", filter);
                    let tbl_scan = LogicalPlan::TableScan(self.table_scan.clone());
                    unfiltered.push(PyExpr::from(filter.clone(), Some(vec![Arc::new(tbl_scan)])))
                }
            }
        }

        Ok(PyFilteredResult {
            io_unfilterable_exprs: unfiltered,
            filtered_exprs: filters
        })
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
