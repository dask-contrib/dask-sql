use std::{sync::Arc, vec};

use datafusion_python::{
    datafusion_common::{DFSchema, ScalarValue},
    datafusion_expr::{
        expr::{Alias, InList},
        logical_plan::TableScan,
        Expr,
        LogicalPlan,
    },
};
use pyo3::prelude::*;

use crate::{
    error::DaskPlannerError,
    expression::{py_expr_list, PyExpr},
    sql::exceptions::py_type_err,
};

#[pyclass(name = "TableScan", module = "dask_sql", subclass)]
#[derive(Clone)]
pub struct PyTableScan {
    pub(crate) table_scan: TableScan,
    input: Arc<LogicalPlan>,
}

type FilterTuple = (String, String, Option<Vec<PyObject>>);
#[pyclass(name = "FilteredResult", module = "dask_sql", subclass)]
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
    pub filtered_exprs: Vec<(PyExpr, FilterTuple)>,
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
        input: &Arc<LogicalPlan>,
        py: Python,
    ) -> Result<Vec<(PyExpr, FilterTuple)>, DaskPlannerError> {
        let mut filter_tuple: Vec<(PyExpr, FilterTuple)> = Vec::new();

        match filter {
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => {
                // Only handle simple Expr(s) for InList operations for now
                if PyTableScan::_valid_expr_type(list) {
                    // While ANSI SQL would not allow for anything other than a Column or Literal
                    // value in this "identifying" `expr` we explicitly check that here just to be sure.
                    // IF it is something else it is returned to Dask to handle
                    let ident = match *expr.clone() {
                        Expr::Column(col) => Ok(col.name),
                        Expr::Alias(Alias { name, .. }) => Ok(name),
                        Expr::Literal(val) => Ok(format!("{}", val)),
                        _ => Err(DaskPlannerError::InvalidIOFilter(format!(
                            "Invalid InList Expr type `{}`. using in Dask instead",
                            filter
                        ))),
                    };

                    let op = if *negated { "not in" } else { "in" };
                    let il: Result<Vec<PyObject>, DaskPlannerError> = list
                        .iter()
                        .map(|f| match f {
                            Expr::Column(col) => Ok(col.name.clone().into_py(py)),
                            Expr::Alias(Alias { name, ..}) => Ok(name.clone().into_py(py)),
                            Expr::Literal(val) => match val {
                                ScalarValue::Boolean(val) => Ok(val.unwrap().into_py(py)),
                                ScalarValue::Float32(val) => Ok(val.unwrap().into_py(py)),
                                ScalarValue::Float64(val) => Ok(val.unwrap().into_py(py)),
                                ScalarValue::Int8(val) => Ok(val.unwrap().into_py(py)),
                                ScalarValue::Int16(val) => Ok(val.unwrap().into_py(py)),
                                ScalarValue::Int32(val) => Ok(val.unwrap().into_py(py)),
                                ScalarValue::Int64(val) => Ok(val.unwrap().into_py(py)),
                                ScalarValue::UInt8(val) => Ok(val.unwrap().into_py(py)),
                                ScalarValue::UInt16(val) => Ok(val.unwrap().into_py(py)),
                                ScalarValue::UInt32(val) => Ok(val.unwrap().into_py(py)),
                                ScalarValue::UInt64(val) => Ok(val.unwrap().into_py(py)),
                                ScalarValue::Utf8(val) => Ok(val.clone().unwrap().into_py(py)),
                                ScalarValue::LargeUtf8(val) => Ok(val.clone().unwrap().into_py(py)),
                                _ => Err(DaskPlannerError::InvalidIOFilter(format!(
                                    "Unsupported ScalarValue `{}` encountered. using in Dask instead",
                                    filter
                                ))),
                            },
                            _ => Ok(f.canonical_name().into_py(py)),
                        })
                        .collect();

                    filter_tuple.push((
                        PyExpr::from(filter.clone(), Some(vec![input.clone()])),
                        (
                            ident.unwrap_or(expr.canonical_name()),
                            op.to_string(),
                            Some(il?),
                        ),
                    ));
                    Ok(filter_tuple)
                } else {
                    let er = DaskPlannerError::InvalidIOFilter(format!(
                        "Invalid identifying column Expr instance `{}`. using in Dask instead",
                        filter
                    ));
                    Err::<Vec<(PyExpr, FilterTuple)>, DaskPlannerError>(er)
                }
            }
            Expr::IsNotNull(expr) => {
                // Only handle simple Expr(s) for IsNotNull operations for now
                let ident = match *expr.clone() {
                    Expr::Column(col) => Ok(col.name),
                    _ => Err(DaskPlannerError::InvalidIOFilter(format!(
                        "Invalid IsNotNull Expr type `{}`. using in Dask instead",
                        filter
                    ))),
                };

                filter_tuple.push((
                    PyExpr::from(filter.clone(), Some(vec![input.clone()])),
                    (
                        ident.unwrap_or(expr.canonical_name()),
                        "is not".to_string(),
                        None,
                    ),
                ));
                Ok(filter_tuple)
            }
            _ => {
                let er = DaskPlannerError::InvalidIOFilter(format!(
                    "Unable to apply filter: `{}` to IO reader, using in Dask instead",
                    filter
                ));
                Err::<Vec<(PyExpr, FilterTuple)>, DaskPlannerError>(er)
            }
        }
    }

    /// Consume the `TableScan` filters (Expr(s)) and convert them into a PyArrow understandable
    /// DNF format that can be directly passed to PyArrow IO readers for Predicate Pushdown. Expr(s)
    /// that cannot be converted to correlating PyArrow IO calls will be returned as is and can be
    /// used in the Python logic to form Dask tasks for the graph to do computational filtering.
    pub fn _expand_dnf_filters(
        input: &Arc<LogicalPlan>,
        filters: &[Expr],
        py: Python,
    ) -> PyFilteredResult {
        let mut filtered_exprs: Vec<(PyExpr, FilterTuple)> = Vec::new();
        let mut unfiltered_exprs: Vec<PyExpr> = Vec::new();

        filters
            .iter()
            .for_each(|f| match PyTableScan::_expand_dnf_filter(f, input, py) {
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
    fn dnf_io_filters(&self, py: Python) -> PyResult<PyFilteredResult> {
        let results = PyTableScan::_expand_dnf_filters(&self.input, &self.table_scan.filters, py);
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
