use datafusion_common::DataFusionError;
use datafusion_sql::sqlparser::parser::ParserError;
use datafusion_sql::sqlparser::tokenizer::TokenizerError;
use pyo3::PyErr;

pub type Result<T> = std::result::Result<T, DaskPlannerError>;

#[derive(Debug)]
pub enum DaskPlannerError {
    DataFusionError(DataFusionError),
    ParserError(ParserError),
    TokenizerError(TokenizerError),
    Internal(String),
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
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", err))
    }
}
