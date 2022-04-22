use crate::sql::types::rel_data_type_field::RelDataTypeField;

use std::collections::HashMap;

use pyo3::prelude::*;

const PRECISION_NOT_SPECIFIED: i32 = i32::MIN;
const SCALE_NOT_SPECIFIED: i32 = -1;

/// RelDataType represents the type of a scalar expression or entire row returned from a relational expression.
#[pyclass]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelDataType {
    nullable: bool,
    field_list: Vec<RelDataTypeField>,
}

/// RelDataType represents the type of a scalar expression or entire row returned from a relational expression.
#[pymethods]
impl RelDataType {
    #[new]
    pub fn new(nullable: bool, fields: Vec<RelDataTypeField>) -> Self {
        Self {
            nullable: nullable,
            field_list: fields,
        }
    }

    /// Looks up a field by name.
    ///
    /// # Arguments
    ///
    /// * `field_name` - A String containing the name of the field to find
    /// * `case_sensitive` - True if column name matching should be case sensitive and false otherwise
    #[pyo3(name = "getField")]
    pub fn field(&self, field_name: String, case_sensitive: bool) -> RelDataTypeField {
        assert!(!self.field_list.is_empty());
        let field_map: HashMap<String, RelDataTypeField> = self.field_map();
        if case_sensitive && field_map.len() > 0 {
            field_map.get(&field_name).unwrap().clone()
        } else {
            for field in &self.field_list {
                if (case_sensitive && field.name().eq(&field_name))
                    || (!case_sensitive && field.name().eq_ignore_ascii_case(&field_name))
                {
                    return field.clone();
                }
            }

            // TODO: Throw a proper error here
            panic!(
                "Unable to find RelDataTypeField with name {:?} in the RelDataType field_list",
                field_name
            );
        }
    }

    /// Returns a map from field names to fields.
    ///
    /// # Notes
    ///
    /// * If several fields have the same name, the map contains the first.
    #[pyo3(name = "getFieldMap")]
    pub fn field_map(&self) -> HashMap<String, RelDataTypeField> {
        let mut fields: HashMap<String, RelDataTypeField> = HashMap::new();
        for field in &self.field_list {
            fields.insert(String::from(field.name()), field.clone());
        }
        fields
    }

    /// Gets the fields in a struct type. The field count is equal to the size of the returned list.
    #[pyo3(name = "getFieldList")]
    pub fn field_list(&self) -> Vec<RelDataTypeField> {
        assert!(!self.field_list.is_empty());
        self.field_list.clone()
    }

    /// Returns the names of the fields in a struct type. The field count
    /// is equal to the size of the returned list.
    #[pyo3(name = "getFieldNames")]
    pub fn field_names(&self) -> Vec<String> {
        assert!(!self.field_list.is_empty());
        let mut field_names: Vec<String> = Vec::new();
        for field in &self.field_list {
            field_names.push(String::from(field.name()));
        }
        field_names
    }

    /// Returns the number of fields in a struct type.
    #[pyo3(name = "getFieldCount")]
    pub fn field_count(&self) -> usize {
        assert!(!self.field_list.is_empty());
        self.field_list.len()
    }

    #[pyo3(name = "isStruct")]
    pub fn is_struct(&self) -> bool {
        self.field_list.len() > 0
    }

    /// Queries whether this type allows null values.
    #[pyo3(name = "isNullable")]
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    #[pyo3(name = "getPrecision")]
    pub fn precision(&self) -> i32 {
        PRECISION_NOT_SPECIFIED
    }

    #[pyo3(name = "getScale")]
    pub fn scale(&self) -> i32 {
        SCALE_NOT_SPECIFIED
    }
}
