use datafusion_python::{
    common::data_type::{DataTypeMap, PyDataType, PythonType, SqlType},
    sql::logical::PyLogicalPlan,
};
use log::debug;
use pyo3::prelude::*;

mod dialect;
mod error;
mod parser;
mod sql;

/// Low-level DataFusion internal package.
///
/// The higher-level public API is defined in pure python files under the
/// dask_planner directory.
#[pymodule]
#[pyo3(name = "rust")]
fn rust(py: Python, m: &PyModule) -> PyResult<()> {
    // Initialize the global Python logger instance
    pyo3_log::init();

    // Register the python classes
    m.add_class::<sql::DaskSQLContext>()?;
    m.add_class::<sql::logical::DaskLogicalPlan>()?;
    m.add_class::<sql::types::RexType>()?;
    m.add_class::<sql::types::DaskTypeMap>()?;
    m.add_class::<sql::types::rel_data_type::RelDataType>()?;
    m.add_class::<sql::statement::PyStatement>()?;
    m.add_class::<sql::schema::DaskSchema>()?;
    m.add_class::<sql::table::DaskTable>()?;
    m.add_class::<sql::function::DaskFunction>()?;
    m.add_class::<sql::table::DaskStatistics>()?;

    // Re-export Arrow DataFusion Python types
    m.add_class::<DataTypeMap>()?;
    m.add_class::<PythonType>()?;
    m.add_class::<PyDataType>()?; // Python wrapper for Arrow DataType
    m.add_class::<PyLogicalPlan>()?;
    m.add_class::<SqlType>()?;

    // Wrapped functions
    m.add_wrapped(wrap_pyfunction!(sql::logical::utils::get_current_node_type))
        .unwrap();
    m.add_wrapped(wrap_pyfunction!(sql::logical::utils::plan_to_table))
        .unwrap();
    m.add_wrapped(wrap_pyfunction!(sql::logical::utils::row_type))
        .unwrap();
    m.add_wrapped(wrap_pyfunction!(sql::logical::utils::named_projects))
        .unwrap();
    m.add_wrapped(wrap_pyfunction!(sql::logical::utils::py_column_name))
        .unwrap();
    m.add_wrapped(wrap_pyfunction!(sql::logical::utils::distinct_agg))
        .unwrap();
    m.add_wrapped(wrap_pyfunction!(sql::logical::utils::sort_ascending))
        .unwrap();
    m.add_wrapped(wrap_pyfunction!(sql::logical::utils::sort_nulls_first))
        .unwrap();
    m.add_wrapped(wrap_pyfunction!(sql::logical::utils::get_filter_expr))
        .unwrap();
    m.add_wrapped(wrap_pyfunction!(sql::logical::utils::get_precision_scale))
        .unwrap();

    // Exceptions
    m.add(
        "DFParsingException",
        py.get_type::<sql::exceptions::ParsingException>(),
    )?;
    m.add(
        "DFOptimizationException",
        py.get_type::<sql::exceptions::OptimizationException>(),
    )?;

    debug!("dask_planner Python module loaded");

    Ok(())
}
