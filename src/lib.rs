use log::debug;
use pyo3::prelude::*;

mod dialect;
mod error;
mod expression;
mod parser;
mod sql;

/// Low-level DataFusion internal package.
///
/// The higher-level public API is defined in pure python files under the
/// dask_planner directory.
#[pymodule]
fn _datafusion_lib(py: Python, m: &PyModule) -> PyResult<()> {
    // Initialize the global Python logger instance
    pyo3_log::init();

    // Register the python classes
    m.add_class::<expression::PyExpr>()?;
    m.add_class::<sql::DaskSQLContext>()?;
    m.add_class::<sql::types::SqlTypeName>()?;
    m.add_class::<sql::types::RexType>()?;
    m.add_class::<sql::types::DaskTypeMap>()?;
    m.add_class::<sql::types::rel_data_type::RelDataType>()?;
    m.add_class::<sql::statement::PyStatement>()?;
    m.add_class::<sql::schema::DaskSchema>()?;
    m.add_class::<sql::table::DaskTable>()?;
    m.add_class::<sql::function::DaskFunction>()?;
    m.add_class::<sql::table::DaskStatistics>()?;
    m.add_class::<sql::logical::PyLogicalPlan>()?;

    // Exceptions
    m.add(
        "DFParsingException",
        py.get_type::<sql::exceptions::ParsingException>(),
    )?;
    m.add(
        "DFOptimizationException",
        py.get_type::<sql::exceptions::OptimizationException>(),
    )?;

    debug!("dask_sql native library loaded");

    Ok(())
}
