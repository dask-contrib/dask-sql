use datafusion_sql::sqlparser::ast::{Expr, TableFactor};

pub struct DaskParserUtils;

impl DaskParserUtils {
    /// Retrieves the table_schema and table_name from a `TableFactor` instance
    pub fn elements_from_tablefactor(tbl_factor: &TableFactor) -> (String, String) {
        match tbl_factor {
            TableFactor::Table {
                name,
                alias: _,
                args: _,
                with_hints: _,
            } => {
                let identities: Vec<String> = name.0.iter().map(|f| f.value.clone()).collect();

                assert!(!identities.is_empty());

                match identities.len() {
                    1 => ("".to_string(), identities[0].clone()),
                    2 => (identities[0].clone(), identities[1].clone()),
                    _ => panic!("TableFactor name only supports 1 or 2 elements"),
                }
            }
            TableFactor::Derived { alias, .. }
            | TableFactor::NestedJoin { alias, .. }
            | TableFactor::TableFunction { alias, .. }
            | TableFactor::UNNEST { alias, .. } => match alias {
                Some(e) => ("".to_string(), e.name.value.clone()),
                None => ("".to_string(), "".to_string()),
            },
        }
    }

    /// Gets the with options from the `TableFactor` instance
    pub fn options_from_tablefactor(tbl_factor: &TableFactor) -> Vec<Expr> {
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
}
