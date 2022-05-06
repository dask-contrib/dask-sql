use datafusion::error::DataFusionError;
use pyo3::{create_exception, PyErr};

create_exception!(rust, ParsingException, pyo3::exceptions::PyException);

pub fn py_type_err(e: DataFusionError) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!("{:?}", e))
}
