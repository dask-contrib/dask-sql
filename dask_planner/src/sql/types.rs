use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};

pub mod rel_data_type;
pub mod rel_data_type_field;

use pyo3::{prelude::*, types::PyDict};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(name = "RexType", module = "datafusion")]
pub enum RexType {
    Literal,
    Call,
    Reference,
    SubqueryAlias,
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
    sql_type: SqlTypeName,
    data_type: PyDataType,
}

/// Functions not exposed to Python
impl DaskTypeMap {
    pub fn from(sql_type: SqlTypeName, data_type: PyDataType) -> Self {
        DaskTypeMap {
            sql_type,
            data_type,
        }
    }
}

#[pymethods]
impl DaskTypeMap {
    #[new]
    #[args(sql_type, py_kwargs = "**")]
    fn new(sql_type: SqlTypeName, py_kwargs: Option<&PyDict>) -> Self {
        let d_type: DataType = match sql_type {
            SqlTypeName::TIMESTAMP_WITH_LOCAL_TIME_ZONE => {
                let (unit, tz) = match py_kwargs {
                    Some(dict) => {
                        let tz: Option<String> = match dict.get_item("tz") {
                            Some(e) => {
                                let res: PyResult<String> = e.extract();
                                Some(res.unwrap())
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
            SqlTypeName::TIMESTAMP => {
                let (unit, tz) = match py_kwargs {
                    Some(dict) => {
                        let tz: Option<String> = match dict.get_item("tz") {
                            Some(e) => {
                                let res: PyResult<String> = e.extract();
                                Some(res.unwrap())
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
            _ => sql_type.to_arrow(),
        };

        DaskTypeMap {
            sql_type,
            data_type: d_type.into(),
        }
    }

    #[pyo3(name = "getSqlType")]
    pub fn sql_type(&self) -> SqlTypeName {
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

/// Enumeration of the type names which can be used to construct a SQL type. Since
/// several SQL types do not exist as Rust types and also because the Enum
/// `SqlTypeName` is already used in the Python Dask-SQL code base this enum is used
/// in place of just using the built-in Rust types.
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(name = "SqlTypeName", module = "datafusion")]
pub enum SqlTypeName {
    ANY,
    ARRAY,
    BIGINT,
    BINARY,
    BOOLEAN,
    CHAR,
    COLUMN_LIST,
    CURSOR,
    DATE,
    DECIMAL,
    DISTINCT,
    DOUBLE,
    DYNAMIC_STAR,
    FLOAT,
    GEOMETRY,
    INTEGER,
    INTERVAL,
    INTERVAL_DAY,
    INTERVAL_DAY_HOUR,
    INTERVAL_DAY_MINUTE,
    INTERVAL_DAY_SECOND,
    INTERVAL_HOUR,
    INTERVAL_HOUR_MINUTE,
    INTERVAL_HOUR_SECOND,
    INTERVAL_MINUTE,
    INTERVAL_MINUTE_SECOND,
    INTERVAL_MONTH,
    INTERVAL_SECOND,
    INTERVAL_YEAR,
    INTERVAL_YEAR_MONTH,
    MAP,
    MULTISET,
    NULL,
    OTHER,
    REAL,
    ROW,
    SARG,
    SMALLINT,
    STRUCTURED,
    SYMBOL,
    TIME,
    TIME_WITH_LOCAL_TIME_ZONE,
    TIMESTAMP,
    TIMESTAMP_WITH_LOCAL_TIME_ZONE,
    TINYINT,
    UNKNOWN,
    VARBINARY,
    VARCHAR,
}

impl SqlTypeName {
    pub fn to_arrow(&self) -> DataType {
        match self {
            SqlTypeName::NULL => DataType::Null,
            SqlTypeName::BOOLEAN => DataType::Boolean,
            SqlTypeName::TINYINT => DataType::Int8,
            SqlTypeName::SMALLINT => DataType::Int16,
            SqlTypeName::INTEGER => DataType::Int32,
            SqlTypeName::BIGINT => DataType::Int64,
            SqlTypeName::REAL => DataType::Float16,
            SqlTypeName::FLOAT => DataType::Float32,
            SqlTypeName::DOUBLE => DataType::Float64,
            SqlTypeName::DATE => DataType::Date64,
            SqlTypeName::VARCHAR => DataType::Utf8,
            _ => {
                todo!("Type: {:?}", self);
            }
        }
    }

    pub fn from_arrow(data_type: &DataType) -> Self {
        match data_type {
            DataType::Null => SqlTypeName::NULL,
            DataType::Boolean => SqlTypeName::BOOLEAN,
            DataType::Int8 => SqlTypeName::TINYINT,
            DataType::Int16 => SqlTypeName::SMALLINT,
            DataType::Int32 => SqlTypeName::INTEGER,
            DataType::Int64 => SqlTypeName::BIGINT,
            DataType::UInt8 => SqlTypeName::TINYINT,
            DataType::UInt16 => SqlTypeName::SMALLINT,
            DataType::UInt32 => SqlTypeName::INTEGER,
            DataType::UInt64 => SqlTypeName::BIGINT,
            DataType::Float16 => SqlTypeName::REAL,
            DataType::Float32 => SqlTypeName::FLOAT,
            DataType::Float64 => SqlTypeName::DOUBLE,
            DataType::Timestamp(_unit, tz) => match tz {
                Some(..) => SqlTypeName::TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                None => SqlTypeName::TIMESTAMP,
            },
            DataType::Date32 => SqlTypeName::DATE,
            DataType::Date64 => SqlTypeName::DATE,
            DataType::Interval(unit) => match unit {
                IntervalUnit::DayTime => SqlTypeName::INTERVAL_DAY,
                IntervalUnit::YearMonth => SqlTypeName::INTERVAL_YEAR_MONTH,
                IntervalUnit::MonthDayNano => SqlTypeName::INTERVAL_MONTH,
            },
            DataType::Binary => SqlTypeName::BINARY,
            DataType::FixedSizeBinary(_size) => SqlTypeName::VARBINARY,
            DataType::Utf8 => SqlTypeName::CHAR,
            DataType::LargeUtf8 => SqlTypeName::VARCHAR,
            DataType::Struct(_fields) => SqlTypeName::STRUCTURED,
            DataType::Decimal128(_precision, _scale) => SqlTypeName::DECIMAL,
            DataType::Decimal256(_precision, _scale) => SqlTypeName::DECIMAL,
            DataType::Map(_field, _bool) => SqlTypeName::MAP,
            _ => todo!(),
        }
    }
}

#[pymethods]
impl SqlTypeName {
    #[pyo3(name = "fromString")]
    #[staticmethod]
    pub fn from_string(input_type: &str) -> Self {
        match input_type {
            "ANY" => SqlTypeName::ANY,
            "ARRAY" => SqlTypeName::ARRAY,
            "NULL" => SqlTypeName::NULL,
            "BOOLEAN" => SqlTypeName::BOOLEAN,
            "COLUMN_LIST" => SqlTypeName::COLUMN_LIST,
            "DISTINCT" => SqlTypeName::DISTINCT,
            "CURSOR" => SqlTypeName::CURSOR,
            "TINYINT" => SqlTypeName::TINYINT,
            "SMALLINT" => SqlTypeName::SMALLINT,
            "INT" => SqlTypeName::INTEGER,
            "INTEGER" => SqlTypeName::INTEGER,
            "BIGINT" => SqlTypeName::BIGINT,
            "REAL" => SqlTypeName::REAL,
            "FLOAT" => SqlTypeName::FLOAT,
            "GEOMETRY" => SqlTypeName::GEOMETRY,
            "DOUBLE" => SqlTypeName::DOUBLE,
            "TIME" => SqlTypeName::TIME,
            "TIME_WITH_LOCAL_TIME_ZONE" => SqlTypeName::TIME_WITH_LOCAL_TIME_ZONE,
            "TIMESTAMP" => SqlTypeName::TIMESTAMP,
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE" => SqlTypeName::TIMESTAMP_WITH_LOCAL_TIME_ZONE,
            "DATE" => SqlTypeName::DATE,
            "INTERVAL" => SqlTypeName::INTERVAL,
            "INTERVAL_DAY" => SqlTypeName::INTERVAL_DAY,
            "INTERVAL_DAY_HOUR" => SqlTypeName::INTERVAL_DAY_HOUR,
            "INTERVAL_DAY_MINUTE" => SqlTypeName::INTERVAL_DAY_MINUTE,
            "INTERVAL_DAY_SECOND" => SqlTypeName::INTERVAL_DAY_SECOND,
            "INTERVAL_HOUR" => SqlTypeName::INTERVAL_HOUR,
            "INTERVAL_HOUR_MINUTE" => SqlTypeName::INTERVAL_HOUR_MINUTE,
            "INTERVAL_HOUR_SECOND" => SqlTypeName::INTERVAL_HOUR_SECOND,
            "INTERVAL_MINUTE" => SqlTypeName::INTERVAL_MINUTE,
            "INTERVAL_MINUTE_SECOND" => SqlTypeName::INTERVAL_MINUTE_SECOND,
            "INTERVAL_MONTH" => SqlTypeName::INTERVAL_MONTH,
            "INTERVAL_SECOND" => SqlTypeName::INTERVAL_SECOND,
            "INTERVAL_YEAR" => SqlTypeName::INTERVAL_YEAR,
            "INTERVAL_YEAR_MONTH" => SqlTypeName::INTERVAL_YEAR_MONTH,
            "MAP" => SqlTypeName::MAP,
            "MULTISET" => SqlTypeName::MULTISET,
            "OTHER" => SqlTypeName::OTHER,
            "ROW" => SqlTypeName::ROW,
            "SARG" => SqlTypeName::SARG,
            "BINARY" => SqlTypeName::BINARY,
            "VARBINARY" => SqlTypeName::VARBINARY,
            "CHAR" => SqlTypeName::CHAR,
            "VARCHAR" => SqlTypeName::VARCHAR,
            "STRUCTURED" => SqlTypeName::STRUCTURED,
            "SYMBOL" => SqlTypeName::SYMBOL,
            "DECIMAL" => SqlTypeName::DECIMAL,
            "DYNAMIC_STAT" => SqlTypeName::DYNAMIC_STAR,
            "UNKNOWN" => SqlTypeName::UNKNOWN,
            _ => unimplemented!("SqlTypeName::from_string() for str type: {}", input_type),
        }
    }
}
