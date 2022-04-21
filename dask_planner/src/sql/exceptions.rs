use pyo3::create_exception;

create_exception!(rust, ParsingException, pyo3::exceptions::PyException);
