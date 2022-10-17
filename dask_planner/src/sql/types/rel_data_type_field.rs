use std::fmt;

use datafusion_common::{DFField, DFSchema};
use pyo3::prelude::*;

use crate::{
    error::Result,
    sql::types::{DaskTypeMap, SqlTypeName},
};

/// RelDataTypeField represents the definition of a field in a structured RelDataType.
#[pyclass(name = "RelDataTypeField", module = "dask_planner", subclass)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelDataTypeField {
    qualifier: Option<String>,
    name: String,
    data_type: DaskTypeMap,
    index: usize,
}

// Functions that should not be presented to Python are placed here
impl RelDataTypeField {
    pub fn from(field: &DFField, schema: &DFSchema) -> Result<RelDataTypeField> {
        let qualifier: Option<&str> = match field.qualifier() {
            Some(qualifier) => Some(qualifier),
            None => None,
        };
        Ok(RelDataTypeField {
            qualifier: qualifier.map(|qualifier| qualifier.to_string()),
            name: field.name().clone(),
            data_type: DaskTypeMap {
                sql_type: SqlTypeName::from_arrow(field.data_type())?,
                data_type: field.data_type().clone().into(),
            },
            index: schema.index_of_column_by_name(qualifier, field.name())?,
        })
    }
}

#[pymethods]
impl RelDataTypeField {
    #[new]
    pub fn new(name: &str, type_map: DaskTypeMap, index: usize) -> Self {
        Self {
            qualifier: None,
            name: name.to_owned(),
            data_type: type_map,
            index,
        }
    }

    #[pyo3(name = "getQualifier")]
    pub fn qualifier(&self) -> Option<String> {
        self.qualifier.clone()
    }

    #[pyo3(name = "getName")]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[pyo3(name = "getQualifiedName")]
    pub fn qualified_name(&self) -> String {
        match &self.qualifier() {
            Some(qualifier) => format!("{}.{}", &qualifier, self.name()),
            None => self.name().to_string(),
        }
    }

    #[pyo3(name = "getIndex")]
    pub fn index(&self) -> usize {
        self.index
    }

    #[pyo3(name = "getType")]
    pub fn data_type(&self) -> DaskTypeMap {
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
    pub fn get_value(&self) -> DaskTypeMap {
        self.data_type()
    }

    #[pyo3(name = "setValue")]
    pub fn set_value(&mut self, data_type: DaskTypeMap) {
        self.data_type = data_type
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
