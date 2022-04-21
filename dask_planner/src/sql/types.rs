use datafusion::arrow::datatypes::{DataType, TimeUnit};

pub mod rel_data_type;
pub mod rel_data_type_field;

use pyo3::prelude::*;

/// Enumeration of the type names which can be used to construct a SQL type. Since
/// several SQL types do not exist as Rust types and also because the Enum
/// `SqlTypeName` is already used in the Python Dask-SQL code base this enum is used
/// in place of just using the built-in Rust types.
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
enum SqlTypeName {
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

/// Takes an Arrow DataType (https://docs.rs/crate/arrow/latest/source/src/datatypes/datatype.rs)
/// and converts it to a SQL type. The SQL type is a String and represents the valid
/// SQL types which are supported by Dask-SQL
pub(crate) fn arrow_type_to_sql_type(arrow_type: DataType) -> String {
    match arrow_type {
        DataType::Null => String::from("NULL"),
        DataType::Boolean => String::from("BOOLEAN"),
        DataType::Int8 => String::from("TINYINT"),
        DataType::UInt8 => String::from("TINYINT"),
        DataType::Int16 => String::from("SMALLINT"),
        DataType::UInt16 => String::from("SMALLINT"),
        DataType::Int32 => String::from("INTEGER"),
        DataType::UInt32 => String::from("INTEGER"),
        DataType::Int64 => String::from("BIGINT"),
        DataType::UInt64 => String::from("BIGINT"),
        DataType::Float32 => String::from("FLOAT"),
        DataType::Float64 => String::from("DOUBLE"),
        DataType::Timestamp(unit, tz) => {
            // let mut timestamp_str: String = "timestamp[".to_string();

            // let unit_str: &str = match unit {
            //     TimeUnit::Microsecond => "ps",
            //     TimeUnit::Millisecond => "ms",
            //     TimeUnit::Nanosecond => "ns",
            //     TimeUnit::Second => "s",
            // };

            // timestamp_str.push_str(&format!("{}", unit_str));
            // match tz {
            //     Some(e) => {
            //         timestamp_str.push_str(&format!(", {}", e))
            //     },
            //     None => (),
            // }
            // timestamp_str.push_str("]");
            // println!("timestamp_str: {:?}", timestamp_str);
            // timestamp_str

            let mut timestamp_str: String = "TIMESTAMP".to_string();
            match tz {
                Some(e) => {
                    timestamp_str.push_str("_WITH_LOCAL_TIME_ZONE(0)");
                }
                None => timestamp_str.push_str("(0)"),
            }
            timestamp_str
        }
        DataType::Date32 => String::from("DATE"),
        DataType::Date64 => String::from("DATE"),
        DataType::Time32(..) => String::from("TIMESTAMP"),
        DataType::Time64(..) => String::from("TIMESTAMP"),
        DataType::Utf8 => String::from("VARCHAR"),
        DataType::LargeUtf8 => String::from("BIGVARCHAR"),
        _ => todo!("Unimplemented Arrow DataType encountered"),
    }
}

/// Takes a valid Dask-SQL type and converts that String representation to an instance
/// of Arrow DataType (https://docs.rs/crate/arrow/latest/source/src/datatypes/datatype.rs)
pub(crate) fn sql_type_to_arrow_type(str_sql_type: String) -> DataType {
    println!("str_sql_type: {:?}", str_sql_type);

    // TODO: https://github.com/dask-contrib/dask-sql/issues/485
    if str_sql_type.starts_with("timestamp") {
        DataType::Timestamp(TimeUnit::Nanosecond, Some(String::from("Europe/Berlin")))
    } else {
        match &str_sql_type[..] {
            "NULL" => DataType::Null,
            "BOOLEAN" => DataType::Boolean,
            "TINYINT" => DataType::Int8,
            "SMALLINT" => DataType::Int16,
            "INTEGER" => DataType::Int32,
            "BIGINT" => DataType::Int64,
            "FLOAT" => DataType::Float32,
            "DOUBLE" => DataType::Float64,
            "VARCHAR" => DataType::Utf8,
            "TIMESTAMP" => DataType::Timestamp(TimeUnit::Nanosecond, None),
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE" => DataType::Timestamp(TimeUnit::Nanosecond, None),
            _ => todo!("Not yet implemented String value: {:?}", &str_sql_type),
        }
    }
}

#[pyclass(name = "DataType", module = "datafusion", subclass)]
#[derive(Debug, Clone)]
pub struct PyDataType {
    pub data_type: DataType,
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
