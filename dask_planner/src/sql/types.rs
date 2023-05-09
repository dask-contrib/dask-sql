pub mod rel_data_type;
pub mod rel_data_type_field;

use datafusion_python::{
    common::data_type::{DataTypeMap, SqlType},
    datafusion::arrow::datatypes::{DataType, TimeUnit},
};
use pyo3::{prelude::*, types::PyDict};

use crate::sql::exceptions::py_type_err;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(name = "RexType", module = "datafusion")]
pub enum RexType {
    Alias,
    Literal,
    Call,
    Reference,
    ScalarSubquery,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(name = "DaskTypeMap", module = "datafusion", subclass)]
/// Represents a Python Data Type. This is needed instead of simple
/// Enum instances because PyO3 can only support unit variants as
/// of version 0.16 which means Enums like `DataType::TIMESTAMP_WITH_LOCAL_TIME_ZONE`
/// which generally hold `unit` and `tz` information are unable to
/// do that so data is lost. This struct aims to solve that issue
/// by taking the type Enum from Python and some optional extra
/// parameters that can be used to properly create those DataType
/// instances in Rust.
pub struct DaskTypeMap {
    sql_type: SqlType,
    data_type: PyDataType,
}

/// Functions not exposed to Python
impl DaskTypeMap {
    pub fn from(sql_type: SqlType, data_type: PyDataType) -> Self {
        DaskTypeMap {
            sql_type,
            data_type,
        }
    }
}

#[pymethods]
impl DaskTypeMap {
    #[new]
    #[pyo3(signature = (sql_type, **py_kwargs))]
    fn new(sql_type: SqlType, py_kwargs: Option<&PyDict>) -> PyResult<Self> {
        let d_type: DataType = match sql_type {
            SqlType::TIMESTAMP_WITH_LOCAL_TIME_ZONE => {
                let (unit, tz) = match py_kwargs {
                    Some(dict) => {
                        let tz = match dict.get_item("tz") {
                            Some(e) => {
                                let res: &str = e.extract().unwrap();
                                Some(res.into())
                            }
                            None => None,
                        };
                        let unit: TimeUnit = match dict.get_item("unit") {
                            Some(e) => {
                                let res: PyResult<&str> = e.extract();
                                match res.unwrap() {
                                    "Second" => TimeUnit::Second,
                                    "Millisecond" => TimeUnit::Millisecond,
                                    "Microsecond" => TimeUnit::Microsecond,
                                    "Nanosecond" => TimeUnit::Nanosecond,
                                    _ => TimeUnit::Nanosecond,
                                }
                            }
                            // Default to Nanosecond which is common if not present
                            None => TimeUnit::Nanosecond,
                        };
                        (unit, tz)
                    }
                    // Default to Nanosecond and None for tz which is common if not present
                    None => (TimeUnit::Nanosecond, None),
                };
                DataType::Timestamp(unit, tz)
            }
            SqlType::TIMESTAMP => {
                let (unit, tz) = match py_kwargs {
                    Some(dict) => {
                        let tz = match dict.get_item("tz") {
                            Some(e) => {
                                let res: &str = e.extract().unwrap();
                                Some(res.into())
                            }
                            None => None,
                        };
                        let unit: TimeUnit = match dict.get_item("unit") {
                            Some(e) => {
                                let res: PyResult<&str> = e.extract();
                                match res.unwrap() {
                                    "Second" => TimeUnit::Second,
                                    "Millisecond" => TimeUnit::Millisecond,
                                    "Microsecond" => TimeUnit::Microsecond,
                                    "Nanosecond" => TimeUnit::Nanosecond,
                                    _ => TimeUnit::Nanosecond,
                                }
                            }
                            // Default to Nanosecond which is common if not present
                            None => TimeUnit::Nanosecond,
                        };
                        (unit, tz)
                    }
                    // Default to Nanosecond and None for tz which is common if not present
                    None => (TimeUnit::Nanosecond, None),
                };
                DataType::Timestamp(unit, tz)
            }
            SqlType::DECIMAL => {
                let (precision, scale) = match py_kwargs {
                    Some(dict) => {
                        let precision: u8 = match dict.get_item("precision") {
                            Some(e) => {
                                let res: PyResult<u8> = e.extract();
                                res.unwrap()
                            }
                            None => 38,
                        };
                        let scale: i8 = match dict.get_item("scale") {
                            Some(e) => {
                                let res: PyResult<i8> = e.extract();
                                res.unwrap()
                            }
                            None => 0,
                        };
                        (precision, scale)
                    }
                    None => (38, 10),
                };
                DataType::Decimal128(precision, scale)
            }
            _ => {
                DataTypeMap::py_map_from_sql_type(&sql_type)?
                    .arrow_type
                    .data_type
            }
        };

        Ok(DaskTypeMap {
            sql_type,
            data_type: d_type.into(),
        })
    }

    fn __str__(&self) -> String {
        format!("{:?}", self.sql_type)
    }

    #[pyo3(name = "getSqlType")]
    pub fn sql_type(&self) -> SqlType {
        self.sql_type.clone()
    }

    #[pyo3(name = "getDataType")]
    pub fn data_type(&self) -> PyDataType {
        self.data_type.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(name = "PyDataType", module = "datafusion", subclass)]
pub struct PyDataType {
    data_type: DataType,
}

#[pymethods]
impl PyDataType {
    /// Gets the precision/scale represented by the PyDataType's decimal datatype
    #[pyo3(name = "getPrecisionScale")]
    pub fn get_precision_scale(&self) -> PyResult<(u8, i8)> {
        Ok(match &self.data_type {
            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                (*precision, *scale)
            }
            _ => {
                return Err(py_type_err(format!(
                    "Catch all triggered in get_precision_scale, {:?}",
                    &self.data_type
                )))
            }
        })
    }
}

impl From<PyDataType> for DataType {
    fn from(data_type: PyDataType) -> DataType {
        data_type.data_type
    }
}

impl From<DataType> for PyDataType {
    fn from(data_type: DataType) -> PyDataType {
        PyDataType { data_type }
    }
}
