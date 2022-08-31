//! SQL Parser
//!
//! Declares a SQL parser based on sqlparser that handles custom formats that we need.

use crate::dialect::DaskDialect;
use crate::sql::parser_utils::DaskParserUtils;
use datafusion_sql::sqlparser::{
    ast::{Expr, SelectItem, Statement as SQLStatement},
    dialect::{keywords::Keyword, Dialect},
    parser::{Parser, ParserError},
    tokenizer::{Token, Tokenizer},
};
use std::collections::VecDeque;

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
    /// IF NOT EXISTS
    pub if_not_exists: bool,
    /// To replace the model or not
    pub or_replace: bool,
    /// with options
    pub with_options: Vec<Expr>,
}

/// Dask-SQL extension DDL for `PREDICT`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredictModel {
    /// model schema
    pub schema_name: String,
    /// model name
    pub name: String,
    /// input Query
    pub select: SQLStatement,
}

/// Dask-SQL extension DDL for `CREATE SCHEMA`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateCatalogSchema {
    /// schema_name
    pub schema_name: String,
    /// if not exists
    pub if_not_exists: bool,
    /// or replace
    pub or_replace: bool,
}

/// Dask-SQL extension DDL for `CREATE TABLE ... WITH`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTable {
    /// table schema, "something" in "something.table_name"
    pub table_schema: String,
    /// table name
    pub name: String,
    /// if not exists
    pub if_not_exists: bool,
    /// or replace
    pub or_replace: bool,
    /// with options
    pub with_options: Vec<Expr>,
}

/// Dask-SQL extension DDL for `CREATE VIEW`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateView {
    /// view schema, "something" in "something.view_name"
    pub view_schema: String,
    /// view name
    pub name: String,
    /// if not exists
    pub if_not_exists: bool,
    /// or replace
    pub or_replace: bool,
}

/// Dask-SQL extension DDL for `DROP MODEL`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropModel {
    /// model name
    pub name: String,
    pub if_exists: bool,
}

/// Dask-SQL extension DDL for `SHOW SCHEMAS`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowSchemas {
    /// like
    pub like: Option<String>,
}

/// Dask-SQL extension DDL for `SHOW TABLES FROM`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowTables {
    /// schema name
    pub schema_name: Option<String>,
}

/// Dask-SQL extension DDL for `SHOW COLUMNS FROM`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowColumns {
    /// Table name
    pub table_name: String,
    pub schema_name: Option<String>,
}

/// Dask-SQL extension DDL for `USE SCHEMA`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropSchema {
    /// schema name
    pub schema_name: String,
    /// if exists
    pub if_exists: bool,
}

/// Dask-SQL extension DDL for `USE SCHEMA`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UseSchema {
    /// schema name
    pub schema_name: String,
}

/// Dask-SQL extension DDL for `ANALYZE TABLE`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyzeTable {
    pub table_name: String,
    pub schema_name: Option<String>,
    pub columns: Vec<String>,
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
    /// Extension: `CREATE SCHEMA`
    CreateCatalogSchema(Box<CreateCatalogSchema>),
    /// Extension: `CREATE TABLE`
    CreateTable(Box<CreateTable>),
    /// Extension: `CREATE VIEW`
    CreateView(Box<CreateView>),
    /// Extension: `DROP MODEL`
    DropModel(Box<DropModel>),
    /// Extension: `PREDICT`
    PredictModel(Box<PredictModel>),
    // Extension: `SHOW SCHEMAS`
    ShowSchemas(Box<ShowSchemas>),
    // Extension: `SHOW TABLES FROM`
    ShowTables(Box<ShowTables>),
    // Extension: `SHOW COLUMNS FROM`
    ShowColumns(Box<ShowColumns>),
    // Exntension: `DROP SCHEMA`
    DropSchema(Box<DropSchema>),
    // Extension: `USE SCHEMA`
    UseSchema(Box<UseSchema>),
    // Extension: `ANALYZE TABLE`
    AnalyzeTable(Box<AnalyzeTable>),
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
                    Keyword::SELECT => {
                        // Check for PREDICT token in statement
                        let mut cnt = 1;
                        loop {
                            match self.parser.next_token() {
                                Token::Word(w) => {
                                    match w.value.to_lowercase().as_str() {
                                        "predict" => {
                                            return self.parse_predict_model();
                                        }
                                        _ => {
                                            // Keep looking for PREDICT
                                            cnt += 1;
                                            continue;
                                        }
                                    }
                                }
                                Token::EOF => {
                                    break;
                                }
                                _ => {
                                    // Keep looking for PREDICT
                                    cnt += 1;
                                    continue;
                                }
                            }
                        }

                        // Reset the parser back to where we started
                        for _ in 0..cnt {
                            self.parser.prev_token();
                        }

                        // use the native parser
                        Ok(DaskStatement::Statement(Box::from(
                            self.parser.parse_statement()?,
                        )))
                    }
                    Keyword::SHOW => {
                        // move one token forward
                        self.parser.next_token();
                        // use custom parsing
                        self.parse_show()
                    }
                    Keyword::USE => {
                        // move one token forwrd
                        self.parser.next_token();
                        // use custom parsing
                        self.parse_use()
                    }
                    Keyword::ANALYZE => {
                        // move one token foward
                        self.parser.next_token();
                        self.parse_analyze()
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

                        let if_not_exists = self.parser.parse_keywords(&[
                            Keyword::IF,
                            Keyword::NOT,
                            Keyword::EXISTS,
                        ]);

                        // use custom parsing
                        self.parse_create_model(if_not_exists, or_replace)
                    }
                    "schema" => {
                        // move one token forward
                        self.parser.next_token();

                        let if_not_exists = self.parser.parse_keywords(&[
                            Keyword::IF,
                            Keyword::NOT,
                            Keyword::EXISTS,
                        ]);

                        // use custom parsing
                        self.parse_create_schema(if_not_exists, or_replace)
                    }
                    "table" => {
                        // move one token forward
                        self.parser.next_token();

                        // use custom parsing
                        self.parse_create_table(true, or_replace)
                    }
                    "view" => {
                        // move one token forward
                        self.parser.next_token();
                        // use custom parsing
                        self.parse_create_table(false, or_replace)
                    }
                    _ => {
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
                    "schema" => {
                        // move one token forward
                        self.parser.next_token();
                        // use custom parsing

                        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);

                        let schema_name = self.parser.parse_identifier();

                        let drop_schema = DropSchema {
                            schema_name: schema_name?.value,
                            if_exists,
                        };
                        Ok(DaskStatement::DropSchema(Box::new(drop_schema)))
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

    /// Parse a SQL SHOW statement
    pub fn parse_show(&mut self) -> Result<DaskStatement, ParserError> {
        match self.parser.peek_token() {
            Token::Word(w) => {
                match w.value.to_lowercase().as_str() {
                    "schemas" => {
                        // move one token forward
                        self.parser.next_token();
                        // use custom parsing
                        self.parse_show_schemas()
                    }
                    "tables" => {
                        // move one token forward
                        self.parser.next_token();

                        // If non ansi ... `FROM {schema_name}` is present custom parse
                        // otherwise use sqlparser-rs
                        match self.parser.peek_token() {
                            Token::Word(w) => {
                                match w.value.to_lowercase().as_str() {
                                    "from" => {
                                        // move one token forward
                                        self.parser.next_token();
                                        // use custom parsing
                                        self.parse_show_tables()
                                    }
                                    _ => {
                                        self.parser.prev_token();
                                        // use the native parser
                                        Ok(DaskStatement::Statement(Box::from(
                                            self.parser.parse_show()?,
                                        )))
                                    }
                                }
                            }
                            _ => self.parse_show_tables(),
                        }
                    }
                    "columns" => {
                        self.parser.next_token();
                        // use custom parsing
                        self.parse_show_columns()
                    }
                    _ => {
                        // use the native parser
                        Ok(DaskStatement::Statement(Box::from(
                            self.parser.parse_show()?,
                        )))
                    }
                }
            }
            _ => {
                // use the native parser
                Ok(DaskStatement::Statement(Box::from(
                    self.parser.parse_show()?,
                )))
            }
        }
    }

    /// Parse a SQL USE SCHEMA statement
    pub fn parse_use(&mut self) -> Result<DaskStatement, ParserError> {
        match self.parser.peek_token() {
            Token::Word(w) => {
                match w.value.to_lowercase().as_str() {
                    "schema" => {
                        // move one token forward
                        self.parser.next_token();
                        // use custom parsing
                        let schema_name = self.parser.parse_identifier();

                        let use_schema = UseSchema {
                            schema_name: schema_name?.value,
                        };
                        Ok(DaskStatement::UseSchema(Box::new(use_schema)))
                    }
                    _ => Ok(DaskStatement::Statement(Box::from(
                        self.parser.parse_show()?,
                    ))),
                }
            }
            _ => Ok(DaskStatement::Statement(Box::from(
                self.parser.parse_show()?,
            ))),
        }
    }

    /// Parse a SQL ANALYZE statement
    pub fn parse_analyze(&mut self) -> Result<DaskStatement, ParserError> {
        match self.parser.peek_token() {
            Token::Word(w) => {
                match w.value.to_lowercase().as_str() {
                    "table" => {
                        // move one token forward
                        self.parser.next_token();
                        // use custom parsing
                        self.parse_analyze_table()
                    }
                    _ => {
                        // use the native parser
                        Ok(DaskStatement::Statement(Box::from(
                            self.parser.parse_analyze()?,
                        )))
                    }
                }
            }
            _ => {
                // use the native parser
                Ok(DaskStatement::Statement(Box::from(
                    self.parser.parse_analyze()?,
                )))
            }
        }
    }

    /// Parse a SQL PREDICT statement
    pub fn parse_predict_model(&mut self) -> Result<DaskStatement, ParserError> {
        // PREDICT(
        //     MODEL model_name,
        //     SQLStatement
        // )
        self.parser.expect_token(&Token::LParen)?;

        let is_model = match self.parser.next_token() {
            Token::Word(w) => matches!(w.value.to_lowercase().as_str(), "model"),
            _ => false,
        };
        if !is_model {
            return Err(ParserError::ParserError(
                "parse_predict_model: Expected `MODEL`".to_string(),
            ));
        }

        let (mdl_schema, mdl_name) =
            DaskParserUtils::elements_from_tablefactor(&self.parser.parse_table_factor()?)?;
        self.parser.expect_token(&Token::Comma)?;

        let sql_statement = self.parser.parse_statement()?;
        self.parser.expect_token(&Token::RParen)?;

        let predict = PredictModel {
            schema_name: mdl_schema,
            name: mdl_name,
            select: sql_statement,
        };
        Ok(DaskStatement::PredictModel(Box::new(predict)))
    }

    /// Parse Dask-SQL CREATE MODEL statement
    fn parse_create_model(
        &mut self,
        if_not_exists: bool,
        or_replace: bool,
    ) -> Result<DaskStatement, ParserError> {
        let model_name = self.parser.parse_object_name()?;
        self.parser.expect_keyword(Keyword::WITH)?;

        // `table_name` has been parsed at this point but is needed in `parse_table_factor`, reset consumption
        self.parser.prev_token();
        self.parser.prev_token();

        let table_factor = self.parser.parse_table_factor()?;
        let with_options = DaskParserUtils::options_from_tablefactor(&table_factor);

        // Parse the "AS" before the SQLStatement
        self.parser.expect_keyword(Keyword::AS)?;

        let create = CreateModel {
            name: model_name.to_string(),
            select: self.parser.parse_statement()?,
            if_not_exists,
            or_replace,
            with_options,
        };
        Ok(DaskStatement::CreateModel(Box::new(create)))
    }

    /// Parse Dask-SQL CREATE {IF NOT EXISTS | OR REPLACE} SCHEMA ... statement
    fn parse_create_schema(
        &mut self,
        if_not_exists: bool,
        or_replace: bool,
    ) -> Result<DaskStatement, ParserError> {
        let schema_name = self.parser.parse_identifier()?.value;

        let create = CreateCatalogSchema {
            schema_name,
            if_not_exists,
            or_replace,
        };
        Ok(DaskStatement::CreateCatalogSchema(Box::new(create)))
    }

    /// Parse Dask-SQL CREATE [OR REPLACE] TABLE ... statement
    ///
    /// # Arguments
    ///
    /// * `is_table` - Whether the "table" is a "TABLE" or "VIEW", True if "TABLE" and False otherwise.
    /// * `or_replace` - True if the "TABLE" or "VIEW" should be replaced and False otherwise
    fn parse_create_table(
        &mut self,
        is_table: bool,
        or_replace: bool,
    ) -> Result<DaskStatement, ParserError> {
        // parse [IF NOT EXISTS] `table_name` AS|WITH
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let _table_name = self.parser.parse_identifier();
        let after_name_token = self.parser.peek_token();

        match after_name_token {
            Token::Word(w) => {
                match w.value.to_lowercase().as_str() {
                    "as" => {
                        self.parser.prev_token();
                        if if_not_exists {
                            // Go back three tokens if IF NOT EXISTS was consumed, native parser consumes these tokens as well
                            self.parser.prev_token();
                            self.parser.prev_token();
                            self.parser.prev_token();
                        }

                        // True if TABLE and False if VIEW
                        match is_table {
                            true => Ok(DaskStatement::Statement(Box::from(
                                self.parser.parse_create_table(or_replace, false, None)?,
                            ))),
                            false => {
                                self.parser.prev_token();
                                Ok(DaskStatement::Statement(Box::from(
                                    self.parser.parse_create_view(or_replace)?,
                                )))
                            }
                        }
                    }
                    "with" => {
                        // `table_name` has been parsed at this point but is needed in `parse_table_factor`, reset consumption
                        self.parser.prev_token();

                        let table_factor = self.parser.parse_table_factor()?;
                        let (tbl_schema, tbl_name) =
                            DaskParserUtils::elements_from_tablefactor(&table_factor)?;
                        let with_options = DaskParserUtils::options_from_tablefactor(&table_factor);

                        let create = CreateTable {
                            table_schema: tbl_schema,
                            name: tbl_name,
                            if_not_exists,
                            or_replace,
                            with_options,
                        };
                        Ok(DaskStatement::CreateTable(Box::new(create)))
                    }
                    _ => self.expected("'as' or 'with'", self.parser.peek_token()),
                }
            }
            _ => {
                self.parser.prev_token();
                if if_not_exists {
                    // Go back three tokens if IF NOT EXISTS was consumed
                    self.parser.prev_token();
                    self.parser.prev_token();
                    self.parser.prev_token();
                }
                // use the native parser
                Ok(DaskStatement::Statement(Box::from(
                    self.parser.parse_create_table(or_replace, false, None)?,
                )))
            }
        }
    }

    /// Parse Dask-SQL DROP MODEL statement
    fn parse_drop_model(&mut self) -> Result<DaskStatement, ParserError> {
        let model_name = self.parser.parse_object_name()?;
        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);

        let drop = DropModel {
            name: model_name.to_string(),
            if_exists,
        };
        Ok(DaskStatement::DropModel(Box::new(drop)))
    }

    /// Parse Dask-SQL SHOW SCHEMAS statement
    fn parse_show_schemas(&mut self) -> Result<DaskStatement, ParserError> {
        // Check for existence of `LIKE` clause
        let like_val = match self.parser.peek_token() {
            Token::Word(w) => {
                match w.keyword {
                    Keyword::LIKE => {
                        // move one token forward
                        self.parser.next_token();
                        // use custom parsing
                        Some(self.parser.parse_identifier()?.value)
                    }
                    _ => None,
                }
            }
            _ => None,
        };

        Ok(DaskStatement::ShowSchemas(Box::new(ShowSchemas {
            like: like_val,
        })))
    }

    /// Parse Dask-SQL SHOW TABLES [FROM] statement
    fn parse_show_tables(&mut self) -> Result<DaskStatement, ParserError> {
        let mut schema_name = None;
        if !self.parser.consume_token(&Token::EOF) {
            schema_name = Some(self.parser.parse_identifier()?.value);
        }
        Ok(DaskStatement::ShowTables(Box::new(ShowTables {
            schema_name,
        })))
    }

    /// Parse Dask-SQL SHOW COLUMNS FROM <table>
    fn parse_show_columns(&mut self) -> Result<DaskStatement, ParserError> {
        self.parser.expect_keyword(Keyword::FROM)?;
        let table_factor = self.parser.parse_table_factor()?;
        let (tbl_schema, tbl_name) = DaskParserUtils::elements_from_tablefactor(&table_factor)?;
        Ok(DaskStatement::ShowColumns(Box::new(ShowColumns {
            table_name: tbl_name,
            schema_name: match tbl_schema.as_str() {
                "" => None,
                _ => Some(tbl_schema),
            },
        })))
    }

    /// Parse Dask-SQL ANALYZE TABLE <table>
    fn parse_analyze_table(&mut self) -> Result<DaskStatement, ParserError> {
        let table_factor = self.parser.parse_table_factor()?;
        // parse_table_factor parses the following keyword as an alias, so we need to go back a token
        // TODO: open an issue in sqlparser around this when possible
        self.parser.prev_token();
        self.parser
            .expect_keywords(&[Keyword::COMPUTE, Keyword::STATISTICS, Keyword::FOR])?;
        let (tbl_schema, tbl_name) = DaskParserUtils::elements_from_tablefactor(&table_factor)?;
        let columns = match self
            .parser
            .parse_keywords(&[Keyword::ALL, Keyword::COLUMNS])
        {
            true => vec![],
            false => {
                self.parser.expect_keyword(Keyword::COLUMNS)?;
                let mut values = vec![];
                for select in self.parser.parse_projection()? {
                    match select {
                        SelectItem::UnnamedExpr(expr) => match expr {
                            Expr::Identifier(ident) => values.push(ident.value),
                            unexpected => {
                                return parser_err!(format!(
                                    "Expected Identifier, found: {}",
                                    unexpected
                                ))
                            }
                        },
                        unexpected => {
                            return parser_err!(format!(
                                "Expected UnnamedExpr, found: {}",
                                unexpected
                            ))
                        }
                    }
                }
                values
            }
        };
        Ok(DaskStatement::AnalyzeTable(Box::new(AnalyzeTable {
            table_name: tbl_name,
            schema_name: match tbl_schema.as_str() {
                "" => None,
                _ => Some(tbl_schema),
            },
            columns,
        })))
    }
}
