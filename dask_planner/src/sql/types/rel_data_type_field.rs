use crate::sql::types;
use crate::sql::types::rel_data_type::RelDataType;

use std::fmt;

use pyo3::prelude::*;

use super::SqlTypeName;

/// RelDataTypeField represents the definition of a field in a structured RelDataType.
#[pyclass]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelDataTypeField {
    name: String,
    data_type: RelDataType,
    index: u8,
}

#[pymethods]
impl RelDataTypeField {
    #[pyo3(name = "getName")]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[pyo3(name = "getIndex")]
    pub fn index(&self) -> u8 {
        self.index
    }

    #[pyo3(name = "getType")]
    pub fn data_type(&self) -> RelDataType {
        self.data_type.clone()
    }

    /// Since this logic is being ported from Java getKey is synonymous with getName.
    /// Alas it is used in certain places so it is implemented here to allow other
    /// places in the code base to not have to change.
    #[pyo3(name = "getKey")]
    pub fn get_key(&self) -> &str {
        self.name()
    }

    /// Since this logic is being ported from Java getValue is synonymous with getType.
    /// Alas it is used in certain places so it is implemented here to allow other
    /// places in the code base to not have to change.
    #[pyo3(name = "getValue")]
    pub fn get_value(&self) -> RelDataType {
        self.data_type()
    }

    // TODO: Uncomment after implementing in RelDataType
    // #[pyo3(name = "isDynamicStar")]
    // pub fn is_dynamic_star(&self) -> bool {
    //     self.data_type.getSqlTypeName() == SqlTypeName.DYNAMIC_STAR
    // }
}

impl fmt::Display for RelDataTypeField {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str("Field: ")?;
        fmt.write_str(&self.name)?;
        fmt.write_str(" - Index: ")?;
        fmt.write_str(&self.index.to_string())?;
        // TODO: Uncomment this after implementing the Display trait in RelDataType
        // fmt.write_str(" - DataType: ")?;
        // fmt.write_str(self.data_type.to_string())?;
        Ok(())
    }
}
