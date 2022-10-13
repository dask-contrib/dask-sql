use core::{iter::Peekable, str::Chars};

use datafusion_sql::sqlparser::dialect::Dialect;

#[derive(Debug)]
pub struct DaskDialect {}

impl Dialect for DaskDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://www.postgresql.org/docs/11/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
        // We don't yet support identifiers beginning with "letters with
        // diacritical marks and non-Latin letters"
        ('a'..='z').contains(&ch) || ('A'..='Z').contains(&ch) || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ('a'..='z').contains(&ch)
            || ('A'..='Z').contains(&ch)
            || ('0'..='9').contains(&ch)
            || ch == '$'
            || ch == '_'
    }

    /// Determine if a character starts a quoted identifier. The default
    /// implementation, accepting "double quoted" ids is both ANSI-compliant
    /// and appropriate for most dialects (with the notable exception of
    /// MySQL, MS SQL, and sqlite). You can accept one of characters listed
    /// in `Word::matching_end_quote` here
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"'
    }
    /// Determine if quoted characters are proper for identifier
    fn is_proper_identifier_inside_quotes(&self, mut _chars: Peekable<Chars<'_>>) -> bool {
        true
    }
    /// Determine if FILTER (WHERE ...) filters are allowed during aggregations
    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }
}
