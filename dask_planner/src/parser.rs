//! SQL Parser
//!
//! Declares a SQL parser based on sqlparser that handles custom formats that we need.

use crate::sql::exceptions::py_type_err;
use crate::sql::types::SqlTypeName;
use pyo3::prelude::*;

use crate::dialect::DaskDialect;
use crate::sql::parser_utils::DaskParserUtils;
use datafusion_sql::sqlparser::{
    ast::{Expr, Ident, SelectItem, Statement as SQLStatement, UnaryOperator, Value},
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CustomExpr {
    Map(Vec<Expr>),
    Multiset(Vec<Expr>),
    Nested(Vec<(String, PySqlArg)>),
}

#[pyclass(name = "SqlArg", module = "datafusion")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PySqlArg {
    expr: Option<Expr>,
    custom: Option<CustomExpr>,
}

impl PySqlArg {
    pub fn new(expr: Option<Expr>, custom: Option<CustomExpr>) -> Self {
        Self { expr, custom }
    }
}

#[pymethods]
impl PySqlArg {
    #[pyo3(name = "isCollection")]
    pub fn is_collection(&self) -> PyResult<bool> {
        match &self.custom {
            Some(CustomExpr::Nested(_)) => Ok(false),
            Some(_) => Ok(true),
            None => match &self.expr {
                Some(expr) => match expr {
                    Expr::Array(_) => Ok(true),
                    _ => Ok(false),
                },
                None => Err(py_type_err(
                    "PySqlArg must contain either a standard or custom AST expression",
                )),
            },
        }
    }

    #[pyo3(name = "isKwargs")]
    pub fn is_kwargs(&self) -> PyResult<bool> {
        match &self.custom {
            Some(CustomExpr::Nested(_)) => Ok(true),
            Some(_) => Ok(false),
            None => Ok(false),
        }
    }

    #[pyo3(name = "getOperandList")]
    pub fn get_operand_list(&self) -> PyResult<Vec<PySqlArg>> {
        match &self.custom {
            Some(custom_expr) => match custom_expr {
                CustomExpr::Map(exprs) | CustomExpr::Multiset(exprs) => Ok(exprs
                    .iter()
                    .map(|e| PySqlArg::new(Some(e.clone()), None))
                    .collect()),
                CustomExpr::Nested(_) => Err(py_type_err("Expected Map or Multiset, got Nested")),
            },
            None => match &self.expr {
                Some(expr) => match expr {
                    Expr::Array(array) => Ok(array
                        .elem
                        .iter()
                        .map(|e| PySqlArg::new(Some(e.clone()), None))
                        .collect()),
                    _ => Ok(vec![]),
                },
                None => Err(py_type_err(
                    "PySqlArg must contain either a standard or custom AST expression",
                )),
            },
        }
    }

    #[pyo3(name = "getKwargs")]
    pub fn get_kwargs(&self) -> PyResult<Vec<(String, PySqlArg)>> {
        match &self.custom {
            Some(CustomExpr::Nested(kwargs)) => Ok(kwargs.clone()),
            _ => Ok(vec![]),
        }
    }

    #[pyo3(name = "getSqlType")]
    pub fn get_sql_type(&self) -> PyResult<SqlTypeName> {
        match &self.custom {
            Some(custom_expr) => match custom_expr {
                CustomExpr::Map(_) => Ok(SqlTypeName::MAP),
                CustomExpr::Multiset(_) => Ok(SqlTypeName::MULTISET),
                CustomExpr::Nested(_) => Err(py_type_err("Expected Map or Multiset, got Nested")),
            },
            None => match &self.expr {
                Some(Expr::Array(_)) => Ok(SqlTypeName::ARRAY),
                Some(Expr::Identifier(Ident { .. })) => Ok(SqlTypeName::VARCHAR),
                Some(Expr::Value(scalar)) => match scalar {
                    Value::Boolean(_) => Ok(SqlTypeName::BOOLEAN),
                    Value::Number(_, false) => Ok(SqlTypeName::BIGINT),
                    Value::SingleQuotedString(_) => Ok(SqlTypeName::VARCHAR),
                    unexpected => Err(py_type_err(format!(
                        "Expected string, got {:?}",
                        unexpected
                    ))),
                },
                Some(Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr,
                }) => match &**expr {
                    Expr::Value(Value::Number(_, false)) => Ok(SqlTypeName::BIGINT),
                    unexpected => Err(py_type_err(format!(
                        "Expected string, got {:?}",
                        unexpected
                    ))),
                },
                Some(unexpected) => Err(py_type_err(format!(
                    "Expected array or scalar, got {:?}",
                    unexpected
                ))),
                None => Err(py_type_err(
                    "PySqlArg must contain either a standard or custom AST expression",
                )),
            },
        }
    }

    #[pyo3(name = "getSqlValue")]
    pub fn get_sql_value(&self) -> PyResult<String> {
        match &self.custom {
            None => match &self.expr {
                Some(Expr::Identifier(Ident { value, .. })) => Ok(value.to_string()),
                Some(Expr::Value(scalar)) => match scalar {
                    Value::Boolean(value) => Ok(if *value {
                        "1".to_string()
                    } else {
                        "".to_string()
                    }),
                    Value::SingleQuotedString(string) => Ok(string.to_string()),
                    Value::Number(value, false) => Ok(value.to_string()),
                    unexpected => Err(py_type_err(format!(
                        "Expected string, got {:?}",
                        unexpected
                    ))),
                },
                Some(Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr,
                }) => match &**expr {
                    Expr::Value(Value::Number(value, false)) => Ok(format!("-{}", value)),
                    unexpected => Err(py_type_err(format!(
                        "Expected string, got {:?}",
                        unexpected
                    ))),
                },
                unexpected => Err(py_type_err(format!(
                    "Expected scalar value, got {:?}",
                    unexpected
                ))),
            },
            unexpected => Err(py_type_err(format!(
                "Expected scalar value, got {:?}",
                unexpected
            ))),
        }
    }
}

/// Dask-SQL extension DDL for `CREATE MODEL`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateModel {
    /// model name
    pub name: String,
    /// input Query
    pub select: DaskStatement,
    /// IF NOT EXISTS
    pub if_not_exists: bool,
    /// To replace the model or not
    pub or_replace: bool,
    /// with options
    pub with_options: Vec<(String, PySqlArg)>,
}

/// Dask-SQL extension DDL for `CREATE EXPERIMENT`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateExperiment {
    /// experiment name
    pub name: String,
    /// input Query
    pub select: DaskStatement,
    /// IF NOT EXISTS
    pub if_not_exists: bool,
    /// To replace the model or not
    pub or_replace: bool,
    /// with options
    pub with_options: Vec<(String, PySqlArg)>,
}

/// Dask-SQL extension DDL for `PREDICT`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredictModel {
    /// model schema
    pub schema_name: String,
    /// model name
    pub name: String,
    /// input Query
    pub select: DaskStatement,
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
    pub with_options: Vec<(String, PySqlArg)>,
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
    /// if exists
    pub if_exists: bool,
}

/// Dask-SQL extension DDL for `EXPORT MODEL`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportModel {
    /// model name
    pub name: String,
    /// with options
    pub with_options: Vec<(String, PySqlArg)>,
}

/// Dask-SQL extension DDL for `DESCRIBE MODEL`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeModel {
    /// model name
    pub name: String,
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

/// Dask-SQL extension DDL for `SHOW MODELS`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowModels;

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
    /// Extension: `CREATE EXPERIMENT`
    CreateExperiment(Box<CreateExperiment>),
    /// Extension: `CREATE SCHEMA`
    CreateCatalogSchema(Box<CreateCatalogSchema>),
    /// Extension: `CREATE TABLE`
    CreateTable(Box<CreateTable>),
    /// Extension: `CREATE VIEW`
    CreateView(Box<CreateView>),
    /// Extension: `DROP MODEL`
    DropModel(Box<DropModel>),
    /// Extension: `EXPORT MODEL`
    ExportModel(Box<ExportModel>),
    /// Extension: `DESCRIBE MODEL`
    DescribeModel(Box<DescribeModel>),
    /// Extension: `PREDICT`
    PredictModel(Box<PredictModel>),
    // Extension: `SHOW SCHEMAS`
    ShowSchemas(Box<ShowSchemas>),
    // Extension: `SHOW TABLES FROM`
    ShowTables(Box<ShowTables>),
    // Extension: `SHOW COLUMNS FROM`
    ShowColumns(Box<ShowColumns>),
    // Extension: `SHOW COLUMNS FROM`
    ShowModels(Box<ShowModels>),
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
                    Keyword::DESCRIBE => {
                        // move one token forwrd
                        self.parser.next_token();
                        // use custom parsing
                        self.parse_describe()
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
                        match w.value.to_lowercase().as_str() {
                            "export" => {
                                // move one token forwrd
                                self.parser.next_token();
                                // use custom parsing
                                self.parse_export_model()
                            }
                            _ => {
                                // use the native parser
                                Ok(DaskStatement::Statement(Box::from(
                                    self.parser.parse_statement()?,
                                )))
                            }
                        }
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
                    "experiment" => {
                        // move one token forward
                        self.parser.next_token();

                        let if_not_exists = self.parser.parse_keywords(&[
                            Keyword::IF,
                            Keyword::NOT,
                            Keyword::EXISTS,
                        ]);

                        // use custom parsing
                        self.parse_create_experiment(if_not_exists, or_replace)
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
                    "schema" => {
                        // move one token forward
                        self.parser.next_token();
                        // use custom parsing

                        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);

                        let schema_name = self.parser.parse_identifier()?;

                        let drop_schema = DropSchema {
                            schema_name: schema_name.value,
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
                    "models" => {
                        self.parser.next_token();
                        Ok(DaskStatement::ShowModels(Box::new(ShowModels)))
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

    /// Parse a SQL DESCRIBE statement
    pub fn parse_describe(&mut self) -> Result<DaskStatement, ParserError> {
        match self.parser.peek_token() {
            Token::Word(w) => {
                match w.value.to_lowercase().as_str() {
                    "model" => {
                        self.parser.next_token();
                        // use custom parsing
                        self.parse_describe_model()
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
                        let schema_name = self.parser.parse_identifier()?;

                        let use_schema = UseSchema {
                            schema_name: schema_name.value,
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

        // Limit our input to  ANALYZE, DESCRIBE, SELECT, SHOW statements
        // TODO: find a more sophisticated way to allow any statement that would return a table
        self.parser.expect_one_of_keywords(&[
            Keyword::SELECT,
            Keyword::DESCRIBE,
            Keyword::SHOW,
            Keyword::ANALYZE,
        ])?;
        self.parser.prev_token();

        let select = self.parse_statement()?;

        self.parser.expect_token(&Token::RParen)?;

        let predict = PredictModel {
            schema_name: mdl_schema,
            name: mdl_name,
            select,
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

        // Parse WITH options
        self.parser.expect_keyword(Keyword::WITH)?;
        self.parser.expect_token(&Token::LParen)?;
        let with_options = self.parse_comma_separated(DaskParser::parse_key_value_pair)?;
        self.parser.expect_token(&Token::RParen)?;

        // Parse the nested query statement
        self.parser.expect_keyword(Keyword::AS)?;
        self.parser.expect_token(&Token::LParen)?;

        // Limit our input to  ANALYZE, DESCRIBE, SELECT, SHOW statements
        // TODO: find a more sophisticated way to allow any statement that would return a table
        self.parser.expect_one_of_keywords(&[
            Keyword::SELECT,
            Keyword::DESCRIBE,
            Keyword::SHOW,
            Keyword::ANALYZE,
        ])?;
        self.parser.prev_token();

        let select = self.parse_statement()?;

        self.parser.expect_token(&Token::RParen)?;

        let create = CreateModel {
            name: model_name.to_string(),
            select,
            if_not_exists,
            or_replace,
            with_options,
        };
        Ok(DaskStatement::CreateModel(Box::new(create)))
    }

    // copied from sqlparser crate and adapted to work with DaskParser
    fn parse_comma_separated<T, F>(&mut self, mut f: F) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut DaskParser<'a>) -> Result<T, ParserError>,
    {
        let mut values = vec![];
        loop {
            values.push(f(self)?);
            if !self.parser.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(values)
    }

    fn parse_key_value_pair(&mut self) -> Result<(String, PySqlArg), ParserError> {
        let key = self.parser.parse_identifier()?;
        self.parser.expect_token(&Token::Eq)?;
        match self.parser.next_token() {
            Token::LParen => {
                let key_value_pairs =
                    self.parse_comma_separated(DaskParser::parse_key_value_pair)?;
                self.parser.expect_token(&Token::RParen)?;
                Ok((
                    key.value,
                    PySqlArg::new(None, Some(CustomExpr::Nested(key_value_pairs))),
                ))
            }
            Token::Word(w) if w.value.to_lowercase().as_str() == "map" => {
                // TODO this does not support map or multiset expressions within the map
                self.parser.expect_token(&Token::LBracket)?;
                let values = self.parser.parse_comma_separated(Parser::parse_expr)?;
                self.parser.expect_token(&Token::RBracket)?;
                Ok((
                    key.value,
                    PySqlArg::new(None, Some(CustomExpr::Map(values))),
                ))
            }
            Token::Word(w) if w.value.to_lowercase().as_str() == "multiset" => {
                // TODO this does not support map or multiset expressions within the multiset
                self.parser.expect_token(&Token::LBracket)?;
                let values = self.parser.parse_comma_separated(Parser::parse_expr)?;
                self.parser.expect_token(&Token::RBracket)?;
                Ok((
                    key.value,
                    PySqlArg::new(None, Some(CustomExpr::Multiset(values))),
                ))
            }
            _ => {
                self.parser.prev_token();
                Ok((
                    key.value,
                    PySqlArg::new(Some(self.parser.parse_expr()?), None),
                ))
            }
        }
    }

    /// Parse Dask-SQL CREATE EXPERIMENT statement
    fn parse_create_experiment(
        &mut self,
        if_not_exists: bool,
        or_replace: bool,
    ) -> Result<DaskStatement, ParserError> {
        let experiment_name = self.parser.parse_object_name()?;

        // Parse WITH options
        self.parser.expect_keyword(Keyword::WITH)?;
        self.parser.expect_token(&Token::LParen)?;
        let with_options = self.parse_comma_separated(DaskParser::parse_key_value_pair)?;
        self.parser.expect_token(&Token::RParen)?;

        // Parse the nested query statement
        self.parser.expect_keyword(Keyword::AS)?;
        self.parser.expect_token(&Token::LParen)?;

        // Limit our input to  ANALYZE, DESCRIBE, SELECT, SHOW statements
        // TODO: find a more sophisticated way to allow any statement that would return a table
        self.parser.expect_one_of_keywords(&[
            Keyword::SELECT,
            Keyword::DESCRIBE,
            Keyword::SHOW,
            Keyword::ANALYZE,
        ])?;
        self.parser.prev_token();

        let select = self.parse_statement()?;

        self.parser.expect_token(&Token::RParen)?;

        let create = CreateExperiment {
            name: experiment_name.to_string(),
            select,
            if_not_exists,
            or_replace,
            with_options,
        };
        Ok(DaskStatement::CreateExperiment(Box::new(create)))
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
                        if is_table {
                            Ok(DaskStatement::Statement(Box::from(
                                self.parser.parse_create_table(or_replace, false, None)?,
                            )))
                        } else {
                            self.parser.prev_token();
                            Ok(DaskStatement::Statement(Box::from(
                                self.parser.parse_create_view(or_replace)?,
                            )))
                        }
                    }
                    "with" => {
                        // `table_name` has been parsed at this point but is needed, reset consumption
                        self.parser.prev_token();

                        // Parse schema and table name
                        let obj_name = self.parser.parse_object_name()?;
                        let (tbl_schema, tbl_name) =
                            DaskParserUtils::elements_from_objectname(&obj_name)?;

                        // Parse WITH options
                        self.parser.expect_keyword(Keyword::WITH)?;
                        self.parser.expect_token(&Token::LParen)?;
                        let with_options =
                            self.parse_comma_separated(DaskParser::parse_key_value_pair)?;
                        self.parser.expect_token(&Token::RParen)?;

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

    /// Parse Dask-SQL EXPORT MODEL statement
    fn parse_export_model(&mut self) -> Result<DaskStatement, ParserError> {
        let is_model = match self.parser.next_token() {
            Token::Word(w) => matches!(w.value.to_lowercase().as_str(), "model"),
            _ => false,
        };
        if !is_model {
            return Err(ParserError::ParserError(
                "parse_export_model: Expected `MODEL`".to_string(),
            ));
        }

        let model_name = self.parser.parse_object_name()?;

        // Parse WITH options
        self.parser.expect_keyword(Keyword::WITH)?;
        self.parser.expect_token(&Token::LParen)?;
        let with_options = self.parse_comma_separated(DaskParser::parse_key_value_pair)?;
        self.parser.expect_token(&Token::RParen)?;

        let export = ExportModel {
            name: model_name.to_string(),
            with_options,
        };
        Ok(DaskStatement::ExportModel(Box::new(export)))
    }

    /// Parse Dask-SQL DROP MODEL statement
    fn parse_drop_model(&mut self) -> Result<DaskStatement, ParserError> {
        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let model_name = self.parser.parse_object_name()?;

        let drop = DropModel {
            name: model_name.to_string(),
            if_exists,
        };
        Ok(DaskStatement::DropModel(Box::new(drop)))
    }

    /// Parse Dask-SQL DESRIBE MODEL statement
    fn parse_describe_model(&mut self) -> Result<DaskStatement, ParserError> {
        let model_name = self.parser.parse_object_name()?;

        let describe = DescribeModel {
            name: model_name.to_string(),
        };
        Ok(DaskStatement::DescribeModel(Box::new(describe)))
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

#[cfg(test)]
mod test {
    use crate::parser::{DaskParser, DaskStatement};

    #[test]
    fn create_model() {
        let sql = r#"CREATE MODEL my_model WITH (
            model_class = 'mock.MagicMock',
            target_column = 'target',
            fit_kwargs = (
                first_arg = 3,
                second_arg = ARRAY [ 1, 2 ],
                third_arg = MAP [ 'a', 1 ],
                forth_arg = MULTISET [ 1, 1, 2, 3 ]
            )
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )"#;
        let statements = DaskParser::parse_sql(sql).unwrap();
        assert_eq!(1, statements.len());

        match &statements[0] {
            DaskStatement::CreateModel(create_model) => {
                // test Debug
                let expected = "[\
                    PySqlKwarg { key: Ident { value: \"model_class\", quote_style: None }, value: Expr(Value(SingleQuotedString(\"mock.MagicMock\"))) }, \
                    PySqlKwarg { key: Ident { value: \"target_column\", quote_style: None }, value: Expr(Value(SingleQuotedString(\"target\"))) }, \
                    PySqlKwarg { key: Ident { value: \"fit_kwargs\", quote_style: None }, value: Nested([\
                        PySqlKwarg { key: Ident { value: \"first_arg\", quote_style: None }, value: Expr(Value(Number(\"3\", false))) }, \
                        PySqlKwarg { key: Ident { value: \"second_arg\", quote_style: None }, value: Expr(Array(Array { elem: [Value(Number(\"1\", false)), Value(Number(\"2\", false))], named: true })) }, \
                        PySqlKwarg { key: Ident { value: \"third_arg\", quote_style: None }, value: Map([Value(SingleQuotedString(\"a\")), Value(Number(\"1\", false))]) }, \
                        PySqlKwarg { key: Ident { value: \"forth_arg\", quote_style: None }, value: Multiset([Value(Number(\"1\", false)), Value(Number(\"1\", false)), Value(Number(\"2\", false)), Value(Number(\"3\", false))]) }\
                    ]) }\
                ]";
                assert_eq!(expected, &format!("{:?}", create_model.with_options));

                // test Display
                let expected = "model_class = 'mock.MagicMock', \
                target_column = 'target', \
                fit_kwargs = (\
                    first_arg = '3', \
                    second_arg = ARRAY[1, 2], \
                    third_arg = MAP [ 'a', 1 ], \
                    forth_arg = MULTISET [ 1, 1, 2, 3 ]\
                )";
                assert_eq!(
                    expected,
                    format!(
                        "{}",
                        create_model
                            .with_options
                            .iter()
                            .map(|pair| format!("{}", pair))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                )
            }
            _ => panic!(),
        }
    }
}
