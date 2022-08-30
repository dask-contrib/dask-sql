use datafusion_common::{DFSchema, SchemaError};
use pyo3::{create_exception, PyErr};
use std::fmt::Debug;

// Identifies expections that occur while attempting to generate a `LogicalPlan` from a SQL string
create_exception!(rust, ParsingException, pyo3::exceptions::PyException);

// Identifies exceptions that occur during attempts to optimization an existing `LogicalPlan`
create_exception!(rust, OptimizationException, pyo3::exceptions::PyException);

pub fn py_type_err(e: impl Debug) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!("{:?}", e))
}

pub fn py_runtime_err(e: impl Debug) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e))
}

pub fn py_parsing_exp(e: impl Debug) -> PyErr {
    PyErr::new::<ParsingException, _>(format!("{:?}", e))
}

pub fn py_optimization_exp(e: impl Debug) -> PyErr {
    PyErr::new::<OptimizationException, _>(format!("{:?}", e))
}

pub fn py_field_not_found(qualifier: Option<&str>, name: &str, schema: &DFSchema) -> PyErr {
    let schema_error = SchemaError::FieldNotFound {
        qualifier: qualifier.map(|s| s.to_string()),
        name: name.to_string(),
        valid_fields: Some(schema.field_names()),
    };
    py_runtime_err(format!("{}", schema_error))
}
