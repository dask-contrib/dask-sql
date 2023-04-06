use core::{iter::Peekable, str::Chars};

use datafusion_sql::sqlparser::{
    ast::{Expr, Function, FunctionArg, FunctionArgExpr, Ident, ObjectName, Value},
    dialect::Dialect,
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

#[derive(Debug)]
pub struct DaskDialect {}

impl Dialect for DaskDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://www.postgresql.org/docs/11/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
        // We don't yet support identifiers beginning with "letters with
        // diacritical marks and non-Latin letters"
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_ascii_lowercase()
            || ch.is_ascii_uppercase()
            || ch.is_ascii_digit()
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

    /// override expression parsing
    fn parse_prefix(&self, parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
        fn parse_expr(parser: &mut Parser) -> Result<Option<Expr>, ParserError> {
            match parser.peek_token().token {
                Token::Word(w) if w.value.to_lowercase() == "ceil" => {
                    // CEIL(d TO DAY)
                    parser.next_token(); // skip ceil
                    parser.expect_token(&Token::LParen)?;
                    let expr = parser.parse_expr()?;
                    if !parser.parse_keyword(Keyword::TO) {
                        // Parse CEIL(expr) as normal
                        parser.prev_token();
                        parser.prev_token();
                        parser.prev_token();
                        return Ok(None);
                    }
                    let time_unit = parser.next_token();
                    parser.expect_token(&Token::RParen)?;

                    // convert to function args
                    let args = vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                            Value::SingleQuotedString(time_unit.to_string()),
                        ))),
                    ];

                    Ok(Some(Expr::Function(Function {
                        name: ObjectName(vec![Ident::new("timestampceil")]),
                        args,
                        over: None,
                        distinct: false,
                        special: false,
                    })))
                }
                Token::Word(w) if w.value.to_lowercase() == "floor" => {
                    // FLOOR(d TO DAY)
                    parser.next_token(); // skip floor
                    parser.expect_token(&Token::LParen)?;
                    let expr = parser.parse_expr()?;
                    if !parser.parse_keyword(Keyword::TO) {
                        // Parse FLOOR(expr) as normal
                        parser.prev_token();
                        parser.prev_token();
                        parser.prev_token();
                        return Ok(None);
                    }
                    let time_unit = parser.next_token();
                    parser.expect_token(&Token::RParen)?;

                    // convert to function args
                    let args = vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                            Value::SingleQuotedString(time_unit.to_string()),
                        ))),
                    ];

                    Ok(Some(Expr::Function(Function {
                        name: ObjectName(vec![Ident::new("timestampfloor")]),
                        args,
                        over: None,
                        distinct: false,
                        special: false,
                    })))
                }
                Token::Word(w) if w.value.to_lowercase() == "timestampadd" => {
                    // TIMESTAMPADD(YEAR, 2, d)
                    parser.next_token(); // skip timestampadd
                    parser.expect_token(&Token::LParen)?;
                    let time_unit = parser.next_token();
                    parser.expect_token(&Token::Comma)?;
                    let n = parser.parse_expr()?;
                    parser.expect_token(&Token::Comma)?;
                    let expr = parser.parse_expr()?;
                    parser.expect_token(&Token::RParen)?;

                    // convert to function args
                    let args = vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                            Value::SingleQuotedString(time_unit.to_string()),
                        ))),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(n)),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)),
                    ];

                    Ok(Some(Expr::Function(Function {
                        name: ObjectName(vec![Ident::new("timestampadd")]),
                        args,
                        over: None,
                        distinct: false,
                        special: false,
                    })))
                }
                Token::Word(w) if w.value.to_lowercase() == "timestampdiff" => {
                    parser.next_token(); // skip timestampdiff
                    parser.expect_token(&Token::LParen)?;
                    let time_unit = parser.next_token();
                    parser.expect_token(&Token::Comma)?;
                    let expr1 = parser.parse_expr()?;
                    parser.expect_token(&Token::Comma)?;
                    let expr2 = parser.parse_expr()?;
                    parser.expect_token(&Token::RParen)?;

                    // convert to function args
                    let args = vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                            Value::SingleQuotedString(time_unit.to_string()),
                        ))),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr1)),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr2)),
                    ];

                    Ok(Some(Expr::Function(Function {
                        name: ObjectName(vec![Ident::new("timestampdiff")]),
                        args,
                        over: None,
                        distinct: false,
                        special: false,
                    })))
                }
                Token::Word(w) if w.value.to_lowercase() == "to_timestamp" => {
                    // TO_TIMESTAMP(d, "%d/%m/%Y")
                    parser.next_token(); // skip to_timestamp
                    parser.expect_token(&Token::LParen)?;
                    let expr = parser.parse_expr()?;
                    let comma = parser.consume_token(&Token::Comma);
                    let time_format = if comma {
                        parser.next_token().to_string()
                    } else {
                        "%Y-%m-%d %H:%M:%S".to_string()
                    };
                    parser.expect_token(&Token::RParen)?;

                    // convert to function args
                    let args = vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                            Value::SingleQuotedString(time_format),
                        ))),
                    ];

                    Ok(Some(Expr::Function(Function {
                        name: ObjectName(vec![Ident::new("dsql_totimestamp")]),
                        args,
                        over: None,
                        distinct: false,
                        special: false,
                    })))
                }
                _ => Ok(None),
            }
        }
        match parse_expr(parser) {
            Ok(Some(expr)) => Some(Ok(expr)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
