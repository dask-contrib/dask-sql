use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};

pub mod rel_data_type;
pub mod rel_data_type_field;

use pyo3::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(name = "RexType", module = "datafusion")]
pub enum RexType {
    Literal,
    Call,
    Reference,
    Other,
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
            SqlTypeName::TIMESTAMP => DataType::Timestamp(TimeUnit::Nanosecond, None),
            _ => todo!(),
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
            DataType::Decimal(_precision, _scale) => SqlTypeName::DECIMAL,
            DataType::Map(_field, _bool) => SqlTypeName::MAP,
            _ => todo!(),
        }
    }

    pub fn from_string(input_type: &str) -> Self {
        match input_type {
            "SqlTypeName.NULL" => SqlTypeName::NULL,
            "SqlTypeName.BOOLEAN" => SqlTypeName::BOOLEAN,
            "SqlTypeName.TINYINT" => SqlTypeName::TINYINT,
            "SqlTypeName.SMALLINT" => SqlTypeName::SMALLINT,
            "SqlTypeName.INTEGER" => SqlTypeName::INTEGER,
            "SqlTypeName.BIGINT" => SqlTypeName::BIGINT,
            "SqlTypeName.REAL" => SqlTypeName::REAL,
            "SqlTypeName.FLOAT" => SqlTypeName::FLOAT,
            "SqlTypeName.DOUBLE" => SqlTypeName::DOUBLE,
            "SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE" => {
                SqlTypeName::TIMESTAMP_WITH_LOCAL_TIME_ZONE
            }
            "SqlTypeName.TIMESTAMP" => SqlTypeName::TIMESTAMP,
            "SqlTypeName.DATE" => SqlTypeName::DATE,
            "SqlTypeName.INTERVAL_DAY" => SqlTypeName::INTERVAL_DAY,
            "SqlTypeName.INTERVAL_YEAR_MONTH" => SqlTypeName::INTERVAL_YEAR_MONTH,
            "SqlTypeName.INTERVAL_MONTH" => SqlTypeName::INTERVAL_MONTH,
            "SqlTypeName.BINARY" => SqlTypeName::BINARY,
            "SqlTypeName.VARBINARY" => SqlTypeName::VARBINARY,
            "SqlTypeName.CHAR" => SqlTypeName::CHAR,
            "SqlTypeName.VARCHAR" => SqlTypeName::VARCHAR,
            "SqlTypeName.STRUCTURED" => SqlTypeName::STRUCTURED,
            "SqlTypeName.DECIMAL" => SqlTypeName::DECIMAL,
            "SqlTypeName.MAP" => SqlTypeName::MAP,
            _ => todo!(),
        }
    }
}
