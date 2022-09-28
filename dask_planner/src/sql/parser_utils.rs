use datafusion_sql::sqlparser::{
    ast::{Expr as SqlParserExpr, TableFactor, Value},
    parser::ParserError,
};

pub struct DaskParserUtils;

impl DaskParserUtils {
    /// Retrieves the table_schema and table_name from a `TableFactor` instance
    pub fn elements_from_tablefactor(
        tbl_factor: &TableFactor,
    ) -> Result<(String, String), ParserError> {
        match tbl_factor {
            TableFactor::Table {
                name,
                alias: _,
                args: _,
                with_hints: _,
            } => {
                let identities: Vec<String> = name.0.iter().map(|f| f.value.clone()).collect();

                match identities.len() {
                    1 => Ok(("".to_string(), identities[0].clone())),
                    2 => Ok((identities[0].clone(), identities[1].clone())),
                    _ => Err(ParserError::ParserError(
                        "TableFactor name only supports 1 or 2 elements".to_string(),
                    )),
                }
            }
            TableFactor::Derived { alias, .. }
            | TableFactor::NestedJoin { alias, .. }
            | TableFactor::TableFunction { alias, .. }
            | TableFactor::UNNEST { alias, .. } => match alias {
                Some(e) => Ok(("".to_string(), e.name.value.clone())),
                None => Ok(("".to_string(), "".to_string())),
            },
        }
    }

    /// Gets the with options from the `TableFactor` instance
    pub fn options_from_tablefactor(tbl_factor: &TableFactor) -> Vec<SqlParserExpr> {
        match tbl_factor {
            TableFactor::Table { with_hints, .. } => with_hints.clone(),
            TableFactor::Derived { .. }
            | TableFactor::NestedJoin { .. }
            | TableFactor::TableFunction { .. }
            | TableFactor::UNNEST { .. } => {
                vec![]
            }
        }
    }

    /// Given a SqlParserExpr instance retrieve the String value from it
    pub fn str_from_expr(expression: SqlParserExpr) -> String {
        match expression {
            SqlParserExpr::Identifier(ident) => ident.value,
            SqlParserExpr::Value(value) => match value {
                Value::SingleQuotedString(e) => e.replace('\'', ""),
                Value::DoubleQuotedString(e) => e.replace('\"', ""),
                Value::Boolean(e) => {
                    if e {
                        "True".to_string()
                    } else {
                        "False".to_string()
                    }
                }
                Value::Number(e, ..) => e,
                _ => unimplemented!("Unimplemented Value type: {:?}", value),
            },
            SqlParserExpr::Nested(nested_expr) => Self::str_from_expr(*nested_expr),
            _ => unimplemented!("Unimplemented SqlParserExpr type: {:?}", expression),
        }
    }
}
