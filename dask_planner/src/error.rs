use datafusion_common::DataFusionError;
use pyo3::PyErr;

pub type Result<T> = std::result::Result<T, DaskPlannerError>;

#[derive(Debug)]
pub enum DaskPlannerError {
    DataFusionError(DataFusionError),
    Internal(String),
}

impl From<DataFusionError> for DaskPlannerError {
    fn from(err: DataFusionError) -> Self {
        Self::DataFusionError(err)
    }
}

impl From<DaskPlannerError> for PyErr {
    fn from(err: DaskPlannerError) -> PyErr {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", err))
    }
}
