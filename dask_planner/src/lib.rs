use mimalloc::MiMalloc;
use pyo3::prelude::*;

mod catalog;
mod errors;
mod expression;
mod functions;
mod sql;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Low-level DataFusion internal package.
///
/// The higher-level public API is defined in pure python files under the
/// dask_planner directory.
#[pymodule]
fn rust(py: Python, m: &PyModule) -> PyResult<()> {
    // Register the python classes
    m.add_class::<catalog::PyCatalog>()?;
    m.add_class::<catalog::PyDatabase>()?;
    m.add_class::<catalog::PyTable>()?;
    m.add_class::<expression::PyExpr>()?;

    // SQL specific classes
    m.add_class::<sql::DaskSQLContext>()?;
    m.add_class::<sql::LogicalPlanGenerator>()?;

    m.add_class::<sql::PyStatement>()?;
    m.add_class::<sql::PyQuery>()?;
    m.add_class::<sql::DaskSchema>()?;
    m.add_class::<sql::DaskTable>()?;
    m.add_class::<sql::DaskFunction>()?;
    m.add_class::<sql::DaskStatistics>()?;
    m.add_class::<sql::DaskSQLNode>()?;
    m.add_class::<sql::PyLogicalPlan>()?;
    m.add_class::<sql::DaskRelDataType>()?;

    Ok(())
}
