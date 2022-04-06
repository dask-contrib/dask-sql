use datafusion::arrow::datatypes::{DataType, TimeUnit};

use pyo3::prelude::*;

#[pyclass]
#[derive(Debug, Clone)]
pub struct DaskRelDataType {
    pub(crate) name: String,
    pub(crate) sql_type: DataType,
}

#[pymethods]
impl DaskRelDataType {
    #[new]
    pub fn new(field_name: String, column_str_sql_type: String) -> Self {
        DaskRelDataType {
            name: field_name,
            sql_type: sql_type_to_arrow_type(column_str_sql_type),
        }
    }

    pub fn get_column_name(&self) -> String {
        String::from(self.name.clone())
    }

    pub fn get_type(&self) -> DataType {
        self.sql_type.clone()
    }
}


/// Takes an Arrow DataType (https://docs.rs/crate/arrow/latest/source/src/datatypes/datatype.rs)
/// and converts it to a SQL type. The SQL type is a String slice and represents the valid
/// SQL types which are supported by Dask-SQL
pub(crate) fn arrow_type_to_sql_type(arrow_type: DataType) -> &'static str {
    match arrow_type {
        DataType::Null => "NULL",
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 => "TINYINT",
        DataType::UInt8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::UInt16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::UInt32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt64 => "BIGINT",
        DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Timestamp{..} => "TIMESTAMP",
        DataType::Date32 => "DATE",
        DataType::Date64 => "DATE",
        DataType::Time32(..) => "TIMESTAMP",
        DataType::Time64(..) => "TIMESTAMP",
        DataType::Utf8 => "VARCHAR",
        DataType::LargeUtf8 => "BIGVARCHAR",
        _ => todo!("Unimplemented Arrow DataType encountered")
    }
}


/// Takes a valid Dask-SQL type and converts that String representation to an instance
/// of Arrow DataType (https://docs.rs/crate/arrow/latest/source/src/datatypes/datatype.rs)
pub(crate) fn sql_type_to_arrow_type(str_sql_type: String) -> DataType {
    match &str_sql_type[..] {
        "NULL" => DataType::Null,
        "BOOLEAN" => DataType::Boolean,
        "TINYINT" => DataType::Int8,
        "SMALLINT" => DataType::Int16,
        "INTEGER" => DataType::Int32,
        "BIGINT" => DataType::Int64,
        "FLOAT" => DataType::Float32,
        "DOUBLE" => DataType::Float64,
        "VARCHAR" =>  DataType::Utf8,
        "TIMESTAMP" => DataType::Timestamp(TimeUnit::Millisecond, Some(String::from("America/New_York"))),
        _ => todo!("Not yet implemented String value: {:?}", &str_sql_type),
    }
}
