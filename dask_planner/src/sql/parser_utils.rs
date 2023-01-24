use datafusion_sql::sqlparser::{ast::ObjectName, parser::ParserError};

pub struct DaskParserUtils;

impl DaskParserUtils {
    /// Retrieves the schema and object name from a `ObjectName` instance
    pub fn elements_from_object_name(
        obj_name: &ObjectName,
    ) -> Result<(Option<String>, String), ParserError> {
        let identities: Vec<String> = obj_name.0.iter().map(|f| f.value.clone()).collect();

        match identities.len() {
            1 => Ok((None, identities[0].clone())),
            2 => Ok((Some(identities[0].clone()), identities[1].clone())),
            _ => Err(ParserError::ParserError(
                "TableFactor name only supports 1 or 2 elements".to_string(),
            )),
        }
    }
}
