//! SQL Parser
//!
//! Declares a SQL parser based on sqlparser that handles custom formats that we need.

use crate::dialect::DaskDialect;
use datafusion_sql::sqlparser::{
    ast::{Expr, Statement as SQLStatement, Value},
    dialect::{keywords::Keyword, Dialect},
    parser::{Parser, ParserError},
    tokenizer::{Token, Tokenizer},
};
use std::collections::{HashMap, VecDeque};

macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string()))
    };
}

/// Dask-SQL extension DDL for `CREATE MODEL`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateModel {
    /// model name
    pub name: String,
    /// input Query
    pub select: SQLStatement,
    /// To replace the model or not
    pub or_replace: bool,
}

/// Dask-SQL extension DDL for `DROP MODEL`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropModel {
    /// model name
    pub name: String,
}

/// Dask-SQL Statement representations.
///
/// Tokens parsed by `DaskParser` are converted into these values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DaskStatement {
    /// ANSI SQL AST node
    Statement(Box<SQLStatement>),
    /// Extension: `CREATE MODEL`
    CreateModel(Box<CreateModel>),
    /// Extension: `DROP MODEL`
    DropModel(Box<DropModel>),
}

/// SQL Parser
pub struct DaskParser<'a> {
    parser: Parser<'a>,
}

impl<'a> DaskParser<'a> {
    #[allow(dead_code)]
    /// Parse the specified tokens
    pub fn new(sql: &str) -> Result<Self, ParserError> {
        let dialect = &DaskDialect {};
        DaskParser::new_with_dialect(sql, dialect)
    }

    /// Parse the specified tokens with dialect
    pub fn new_with_dialect(sql: &str, dialect: &'a dyn Dialect) -> Result<Self, ParserError> {
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize()?;

        Ok(DaskParser {
            parser: Parser::new(tokens, dialect),
        })
    }

    #[allow(dead_code)]
    /// Parse a SQL statement and produce a set of statements with dialect
    pub fn parse_sql(sql: &str) -> Result<VecDeque<DaskStatement>, ParserError> {
        let dialect = &DaskDialect {};
        DaskParser::parse_sql_with_dialect(sql, dialect)
    }

    /// Parse a SQL statement and produce a set of statements
    pub fn parse_sql_with_dialect(
        sql: &str,
        dialect: &dyn Dialect,
    ) -> Result<VecDeque<DaskStatement>, ParserError> {
        let mut parser = DaskParser::new_with_dialect(sql, dialect)?;
        let mut stmts = VecDeque::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return parser.expected("end of statement", parser.parser.peek_token());
            }

            let statement = parser.parse_statement()?;
            stmts.push_back(statement);
            expecting_statement_delimiter = true;
        }
        Ok(stmts)
    }

    /// Report unexpected token
    fn expected<T>(&self, expected: &str, found: Token) -> Result<T, ParserError> {
        parser_err!(format!("Expected {}, found: {}", expected, found))
    }

    /// Parse a new expression
    pub fn parse_statement(&mut self) -> Result<DaskStatement, ParserError> {
        match self.parser.peek_token() {
            Token::Word(w) => {
                match w.keyword {
                    Keyword::CREATE => {
                        // move one token forward
                        self.parser.next_token();
                        // use custom parsing
                        self.parse_create()
                    }
                    Keyword::DROP => {
                        // move one token forward
                        self.parser.next_token();
                        // use custom parsing
                        self.parse_drop()
                    }
                    _ => {
                        // use the native parser
                        Ok(DaskStatement::Statement(Box::from(
                            self.parser.parse_statement()?,
                        )))
                    }
                }
            }
            _ => {
                // use the native parser
                Ok(DaskStatement::Statement(Box::from(
                    self.parser.parse_statement()?,
                )))
            }
        }
    }

    /// Parse a SQL CREATE statement
    pub fn parse_create(&mut self) -> Result<DaskStatement, ParserError> {
        let or_replace = self.parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
        match self.parser.peek_token() {
            Token::Word(w) => {
                match w.value.to_lowercase().as_str() {
                    "model" => {
                        // move one token forward
                        self.parser.next_token();
                        // use custom parsing
                        self.parse_create_model(or_replace)
                    }
                    _ => {
                        if or_replace {
                            // Go back two tokens if OR REPLACE was consumed
                            self.parser.prev_token();
                            self.parser.prev_token();
                        }
                        // use the native parser
                        Ok(DaskStatement::Statement(Box::from(
                            self.parser.parse_create()?,
                        )))
                    }
                }
            }
            _ => {
                if or_replace {
                    // Go back two tokens if OR REPLACE was consumed
                    self.parser.prev_token();
                    self.parser.prev_token();
                }
                // use the native parser
                Ok(DaskStatement::Statement(Box::from(
                    self.parser.parse_create()?,
                )))
            }
        }
    }

    /// Parse a SQL DROP statement
    pub fn parse_drop(&mut self) -> Result<DaskStatement, ParserError> {
        match self.parser.peek_token() {
            Token::Word(w) => {
                match w.value.to_lowercase().as_str() {
                    "model" => {
                        // move one token forward
                        self.parser.next_token();
                        // use custom parsing
                        self.parse_drop_model()
                    }
                    _ => {
                        // use the native parser
                        Ok(DaskStatement::Statement(Box::from(
                            self.parser.parse_drop()?,
                        )))
                    }
                }
            }
            _ => {
                // use the native parser
                Ok(DaskStatement::Statement(Box::from(
                    self.parser.parse_drop()?,
                )))
            }
        }
    }

    /// Parse Dask-SQL CREATE MODEL statement
    fn parse_create_model(&mut self, or_replace: bool) -> Result<DaskStatement, ParserError> {
        let model_name = self.parser.parse_object_name()?;
        self.parser.expect_keyword(Keyword::WITH)?;
        self.parser.expect_token(&Token::LParen)?;

        // Parse all KV pairs into a Vec<BinaryExpr> instances
        let kv_binexprs = self.parser.parse_comma_separated(Parser::parse_expr)?;

        let _kv_pairs: Vec<(String, &Box<Expr>)> = kv_binexprs
            .iter()
            .map(|f| match f {
                Expr::BinaryOp { left, op: _, right } => match *left.clone() {
                    Expr::Value(value) => match value {
                        Value::EscapedStringLiteral(key_val)
                        | Value::SingleQuotedString(key_val)
                        | Value::DoubleQuotedString(key_val) => Ok((key_val, right)),
                        _ => Ok(("".to_string(), right)),
                    },
                    _ => Ok(("".to_string(), right)),
                },
                _ => parser_err!(format!("Expected BinaryOp, Key/Value pairs, found: {}", f)),
            })
            .collect::<Result<Vec<_>, ParserError>>()?;
        let _kv_pairs: HashMap<String, &Box<Expr>> = _kv_pairs.into_iter().collect();

        self.parser.expect_token(&Token::RParen)?;

        // Parse the "AS" before the SQLStatement
        self.parser.expect_keyword(Keyword::AS)?;

        let create = CreateModel {
            name: model_name.to_string(),
            select: self.parser.parse_statement()?,
            or_replace,
        };
        Ok(DaskStatement::CreateModel(Box::new(create)))
    }

    /// Parse Dask-SQL DROP MODEL statement
    fn parse_drop_model(&mut self) -> Result<DaskStatement, ParserError> {
        let model_name = self.parser.parse_object_name()?;

        let drop = DropModel {
            name: model_name.to_string(),
        };
        Ok(DaskStatement::DropModel(Box::new(drop)))
    }
}
