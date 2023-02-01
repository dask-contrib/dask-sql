use std::fmt::{Display, Formatter};

use datafusion_common::DataFusionError;
use datafusion_sql::sqlparser::{parser::ParserError, tokenizer::TokenizerError};
use pyo3::PyErr;

pub type Result<T> = std::result::Result<T, DaskPlannerError>;

#[derive(Debug)]
pub enum DaskPlannerError {
    DataFusionError(DataFusionError),
    ParserError(ParserError),
    TokenizerError(TokenizerError),
    Internal(String),
}

impl Display for DaskPlannerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DataFusionError(e) => write!(f, "DataFusion Error: {e}"),
            Self::ParserError(e) => write!(f, "SQL Parser Error: {e}"),
            Self::TokenizerError(e) => write!(f, "SQL Tokenizer Error: {e}"),
            Self::Internal(e) => write!(f, "Internal Error: {e}"),
        }
    }
}

impl From<TokenizerError> for DaskPlannerError {
    fn from(err: TokenizerError) -> Self {
        Self::TokenizerError(err)
    }
}

impl From<ParserError> for DaskPlannerError {
    fn from(err: ParserError) -> Self {
        Self::ParserError(err)
    }
}

impl From<DataFusionError> for DaskPlannerError {
    fn from(err: DataFusionError) -> Self {
        Self::DataFusionError(err)
    }
}

impl From<DaskPlannerError> for PyErr {
    fn from(err: DaskPlannerError) -> PyErr {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{err:?}"))
    }
}
