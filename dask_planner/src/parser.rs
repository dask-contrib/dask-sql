//! SQL Parser
//!
//! Declares a SQL parser based on sqlparser that handles custom formats that we need.

use crate::dialect::DaskDialect;
use datafusion_sql::sqlparser::{
    ast::{ColumnDef, ColumnOptionDef, Statement as SQLStatement, TableConstraint},
    dialect::{keywords::Keyword, Dialect},
    parser::{Parser, ParserError},
    tokenizer::{Token, Tokenizer},
};
use std::collections::VecDeque;

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string()))
    };
}

/// DataFusion extension DDL for `CREATE EXTERNAL TABLE`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateModel {
    /// model name
    pub name: String,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
}

/// DataFusion Statement representations.
///
/// Tokens parsed by `DFParser` are converted into these values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DaskStatement {
    /// ANSI SQL AST node
    Statement(Box<SQLStatement>),
    /// Extension: `CREATE MODEL`
    CreateModel(CreateModel),
}

/// SQL Parser
pub struct DaskParser<'a> {
    parser: Parser<'a>,
}

impl<'a> DaskParser<'a> {
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
        if let Ok(ident) = self.parser.parse_identifier() {
            println!("Identity value is: {:?}", ident.value);
            self.parse_create_model()
        } else {
            println!("Wasn't a match ... default to the native sqlparser parser");
            Ok(DaskStatement::Statement(Box::from(
                self.parser.parse_create()?,
            )))
        }
    }

    // This is a copy of the equivalent implementation in sqlparser.
    fn parse_columns(&mut self) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>), ParserError> {
        let mut columns = vec![];
        let mut constraints = vec![];
        if !self.parser.consume_token(&Token::LParen) || self.parser.consume_token(&Token::RParen) {
            return Ok((columns, constraints));
        }

        loop {
            if let Some(constraint) = self.parser.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Token::Word(_) = self.parser.peek_token() {
                let column_def = self.parse_column_def()?;
                columns.push(column_def);
            } else {
                return self.expected(
                    "column name or constraint definition",
                    self.parser.peek_token(),
                );
            }
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after column definition",
                    self.parser.peek_token(),
                );
            }
        }

        Ok((columns, constraints))
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef, ParserError> {
        let name = self.parser.parse_identifier()?;
        let data_type = self.parser.parse_data_type()?;
        let collation = if self.parser.parse_keyword(Keyword::COLLATE) {
            Some(self.parser.parse_object_name()?)
        } else {
            None
        };
        let mut options = vec![];
        loop {
            if self.parser.parse_keyword(Keyword::CONSTRAINT) {
                let name = Some(self.parser.parse_identifier()?);
                if let Some(option) = self.parser.parse_optional_column_option()? {
                    options.push(ColumnOptionDef { name, option });
                } else {
                    return self.expected(
                        "constraint details after CONSTRAINT <name>",
                        self.parser.peek_token(),
                    );
                }
            } else if let Some(option) = self.parser.parse_optional_column_option()? {
                options.push(ColumnOptionDef { name: None, option });
            } else {
                break;
            };
        }
        Ok(ColumnDef {
            name,
            data_type,
            collation,
            options,
        })
    }

    fn parse_create_model(&mut self) -> Result<DaskStatement, ParserError> {
        self.parser.expect_keyword(Keyword::TABLE)?;
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parser.parse_object_name()?;
        let (columns, _) = self.parse_columns()?;
        self.parser
            .expect_keywords(&[Keyword::STORED, Keyword::AS])?;

        let create = CreateModel {
            name: table_name.to_string(),
            if_not_exists,
        };
        Ok(DaskStatement::CreateModel(create))
    }

    fn consume_token(&mut self, expected: &Token) -> bool {
        let token = self.parser.peek_token().to_string().to_uppercase();
        let token = Token::make_keyword(&token);
        if token == *expected {
            self.parser.next_token();
            true
        } else {
            false
        }
    }
}
