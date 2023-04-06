use std::{convert::From, sync::Arc};

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Column, DFField, DFSchema, ScalarValue};
use datafusion_expr::{
    expr::{AggregateFunction, BinaryExpr, Cast, Sort, TryCast, WindowFunction},
    lit,
    utils::exprlist_to_fields,
    Between,
    BuiltinScalarFunction,
    Case,
    Expr,
    GetIndexedField,
    Like,
    LogicalPlan,
    Operator,
};
use pyo3::prelude::*;

use crate::{
    error::{DaskPlannerError, Result},
    sql::{
        exceptions::{py_runtime_err, py_type_err},
        logical,
        types::RexType,
    },
};

/// An PyExpr that can be used on a DataFrame
#[pyclass(name = "Expression", module = "datafusion", subclass)]
#[derive(Debug, Clone)]
pub struct PyExpr {
    pub expr: Expr,
    // Why a Vec here? Because BinaryExpr on Join might have multiple LogicalPlans
    pub input_plan: Option<Vec<Arc<LogicalPlan>>>,
}

impl From<PyExpr> for Expr {
    fn from(expr: PyExpr) -> Expr {
        expr.expr
    }
}

#[pyclass(name = "ScalarValue", module = "datafusion", subclass)]
#[derive(Debug, Clone)]
pub struct PyScalarValue {
    pub scalar_value: ScalarValue,
}

impl From<PyScalarValue> for ScalarValue {
    fn from(pyscalar: PyScalarValue) -> ScalarValue {
        pyscalar.scalar_value
    }
}

impl From<ScalarValue> for PyScalarValue {
    fn from(scalar_value: ScalarValue) -> PyScalarValue {
        PyScalarValue { scalar_value }
    }
}

/// Convert a list of DataFusion Expr to PyExpr
pub fn py_expr_list(input: &Arc<LogicalPlan>, expr: &[Expr]) -> PyResult<Vec<PyExpr>> {
    Ok(expr
        .iter()
        .map(|e| PyExpr::from(e.clone(), Some(vec![input.clone()])))
        .collect())
}

impl PyExpr {
    /// Generally we would implement the `From` trait offered by Rust
    /// However in this case Expr does not contain the contextual
    /// `LogicalPlan` instance that we need so we need to make a instance
    /// function to take and create the PyExpr.
    pub fn from(expr: Expr, input: Option<Vec<Arc<LogicalPlan>>>) -> PyExpr {
        PyExpr {
            input_plan: input,
            expr,
        }
    }

    /// Determines the name of the `Expr` instance by examining the LogicalPlan
    pub fn _column_name(&self, plan: &LogicalPlan) -> Result<String> {
        let field = expr_to_field(&self.expr, plan)?;
        Ok(field.qualified_column().flat_name())
    }

    fn _rex_type(&self, expr: &Expr) -> RexType {
        match expr {
            Expr::Alias(..) => RexType::Alias,
            Expr::Column(..) | Expr::QualifiedWildcard { .. } | Expr::GetIndexedField { .. } => {
                RexType::Reference
            }
            Expr::ScalarVariable(..) | Expr::Literal(..) => RexType::Literal,
            Expr::BinaryExpr { .. }
            | Expr::Not(..)
            | Expr::IsNotNull(..)
            | Expr::Negative(..)
            | Expr::IsNull(..)
            | Expr::Like { .. }
            | Expr::ILike { .. }
            | Expr::SimilarTo { .. }
            | Expr::Between { .. }
            | Expr::Case { .. }
            | Expr::Cast { .. }
            | Expr::TryCast { .. }
            | Expr::Sort { .. }
            | Expr::ScalarFunction { .. }
            | Expr::AggregateFunction { .. }
            | Expr::WindowFunction { .. }
            | Expr::AggregateUDF { .. }
            | Expr::InList { .. }
            | Expr::Wildcard
            | Expr::ScalarUDF { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::GroupingSet(..)
            | Expr::IsTrue(..)
            | Expr::IsFalse(..)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(..)
            | Expr::IsNotFalse(..)
            | Expr::Placeholder { .. }
            | Expr::IsNotUnknown(_) => RexType::Call,
            Expr::ScalarSubquery(..) => RexType::ScalarSubquery,
        }
    }
}

macro_rules! extract_scalar_value {
    ($self: expr, $variant: ident) => {
        match $self.get_scalar_value()? {
            ScalarValue::$variant(value) => Ok(*value),
            other => Err(unexpected_literal_value(other)),
        }
    };
}

#[pymethods]
impl PyExpr {
    #[staticmethod]
    pub fn literal(value: PyScalarValue) -> PyExpr {
        PyExpr::from(lit(value.scalar_value), None)
    }

    /// Extracts the LogicalPlan from a Subquery, or supported Subquery sub-type, from
    /// the expression instance
    #[pyo3(name = "getSubqueryLogicalPlan")]
    pub fn subquery_plan(&self) -> PyResult<logical::PyLogicalPlan> {
        match &self.expr {
            Expr::ScalarSubquery(subquery) => Ok(subquery.subquery.as_ref().clone().into()),
            _ => Err(py_type_err(format!(
                "Attempted to extract a LogicalPlan instance from invalid Expr {:?}.
                Only Subquery and related variants are supported for this operation.",
                &self.expr
            ))),
        }
    }

    /// If this Expression instances references an existing
    /// Column in the SQL parse tree or not
    #[pyo3(name = "isInputReference")]
    pub fn is_input_reference(&self) -> PyResult<bool> {
        Ok(matches!(&self.expr, Expr::Column(_col)))
    }

    #[pyo3(name = "toString")]
    pub fn to_string(&self) -> PyResult<String> {
        Ok(format!("{}", &self.expr))
    }

    /// Gets the positional index of the Expr instance from the LogicalPlan DFSchema
    #[pyo3(name = "getIndex")]
    pub fn index(&self) -> PyResult<usize> {
        let input: &Option<Vec<Arc<LogicalPlan>>> = &self.input_plan;
        match input {
            Some(input_plans) if !input_plans.is_empty() => {
                let mut schema: DFSchema = (**input_plans[0].schema()).clone();
                for plan in input_plans.iter().skip(1) {
                    schema.merge(plan.schema().as_ref());
                }
                let name = get_expr_name(&self.expr).map_err(py_runtime_err)?;
                schema
                    .index_of_column(&Column::from_qualified_name(name.clone()))
                    .or_else(|_| {
                        // Handles cases when from_qualified_name doesn't format the Column correctly.
                        // Here, we split the name string and grab the relation/table names
                        let split_name: Vec<&str> = name.split('.').collect();
                        let relation = &split_name.first();
                        let table = &split_name.get(1);
                        let col = Column {
                            relation: Some(relation.unwrap().to_string()),
                            name: table.unwrap().to_string(),
                        };
                        schema.index_of_column(&col).map_err(py_runtime_err)
                    })
            }
            _ => Err(py_runtime_err(
                "We need a valid LogicalPlan instance to get the Expr's index in the schema",
            )),
        }
    }

    /// Examine the current/"self" PyExpr and return its "type"
    /// In this context a "type" is what Dask-SQL Python
    /// RexConverter plugin instance should be invoked to handle
    /// the Rex conversion
    #[pyo3(name = "getExprType")]
    pub fn get_expr_type(&self) -> PyResult<String> {
        Ok(String::from(match &self.expr {
            Expr::Alias(..)
            | Expr::Column(..)
            | Expr::Literal(..)
            | Expr::BinaryExpr { .. }
            | Expr::Between { .. }
            | Expr::Cast { .. }
            | Expr::Sort { .. }
            | Expr::ScalarFunction { .. }
            | Expr::AggregateFunction { .. }
            | Expr::InList { .. }
            | Expr::InSubquery { .. }
            | Expr::ScalarUDF { .. }
            | Expr::AggregateUDF { .. }
            | Expr::Exists { .. }
            | Expr::ScalarSubquery(..)
            | Expr::QualifiedWildcard { .. }
            | Expr::Not(..)
            | Expr::GroupingSet(..) => self.expr.variant_name(),
            Expr::ScalarVariable(..)
            | Expr::IsNotNull(..)
            | Expr::Negative(..)
            | Expr::GetIndexedField { .. }
            | Expr::IsNull(..)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::Like { .. }
            | Expr::ILike { .. }
            | Expr::SimilarTo { .. }
            | Expr::IsNotUnknown(_)
            | Expr::Case { .. }
            | Expr::TryCast { .. }
            | Expr::WindowFunction { .. }
            | Expr::Placeholder { .. }
            | Expr::Wildcard => {
                return Err(py_type_err(format!(
                    "Encountered unsupported expression type: {}",
                    &self.expr.variant_name()
                )))
            }
        }))
    }

    /// Determines the type of this Expr based on its variant
    #[pyo3(name = "getRexType")]
    pub fn rex_type(&self) -> PyResult<RexType> {
        Ok(self._rex_type(&self.expr))
    }

    /// Python friendly shim code to get the name of a column referenced by an expression
    pub fn column_name(&self, mut plan: logical::PyLogicalPlan) -> PyResult<String> {
        self._column_name(&plan.current_node())
            .map_err(py_runtime_err)
    }

    /// Row expressions, Rex(s), operate on the concept of operands. This maps to expressions that are used in
    /// the "call" logic of the Dask-SQL python codebase. Different variants of Expressions, Expr(s),
    /// store those operands in different datastructures. This function examines the Expr variant and returns
    /// the operands to the calling logic as a Vec of PyExpr instances.
    #[pyo3(name = "getOperands")]
    pub fn get_operands(&self) -> PyResult<Vec<PyExpr>> {
        match &self.expr {
            // Expr variants that are themselves the operand to return
            Expr::Column(..) | Expr::ScalarVariable(..) | Expr::Literal(..) => {
                Ok(vec![PyExpr::from(
                    self.expr.clone(),
                    self.input_plan.clone(),
                )])
            }

            // Expr(s) that house the Expr instance to return in their bounded params
            Expr::Alias(expr, ..)
            | Expr::Not(expr)
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsNotUnknown(expr)
            | Expr::Negative(expr)
            | Expr::GetIndexedField(GetIndexedField { expr, .. })
            | Expr::Cast(Cast { expr, .. })
            | Expr::TryCast(TryCast { expr, .. })
            | Expr::Sort(Sort { expr, .. })
            | Expr::InSubquery { expr, .. } => {
                Ok(vec![PyExpr::from(*expr.clone(), self.input_plan.clone())])
            }

            // Expr variants containing a collection of Expr(s) for operands
            Expr::AggregateFunction(AggregateFunction { args, .. })
            | Expr::AggregateUDF { args, .. }
            | Expr::ScalarFunction { args, .. }
            | Expr::ScalarUDF { args, .. }
            | Expr::WindowFunction(WindowFunction { args, .. }) => Ok(args
                .iter()
                .map(|arg| PyExpr::from(arg.clone(), self.input_plan.clone()))
                .collect()),

            // Expr(s) that require more specific processing
            Expr::Case(Case {
                expr,
                when_then_expr,
                else_expr,
            }) => {
                let mut operands: Vec<PyExpr> = Vec::new();

                if let Some(e) = expr {
                    operands.push(PyExpr::from(*e.clone(), self.input_plan.clone()));
                };

                for (when, then) in when_then_expr {
                    operands.push(PyExpr::from(*when.clone(), self.input_plan.clone()));
                    operands.push(PyExpr::from(*then.clone(), self.input_plan.clone()));
                }

                if let Some(e) = else_expr {
                    operands.push(PyExpr::from(*e.clone(), self.input_plan.clone()));
                };

                Ok(operands)
            }
            Expr::InList { expr, list, .. } => {
                let mut operands: Vec<PyExpr> =
                    vec![PyExpr::from(*expr.clone(), self.input_plan.clone())];
                for list_elem in list {
                    operands.push(PyExpr::from(list_elem.clone(), self.input_plan.clone()));
                }

                Ok(operands)
            }
            Expr::BinaryExpr(BinaryExpr { left, right, .. }) => Ok(vec![
                PyExpr::from(*left.clone(), self.input_plan.clone()),
                PyExpr::from(*right.clone(), self.input_plan.clone()),
            ]),
            Expr::Like(Like { expr, pattern, .. }) => Ok(vec![
                PyExpr::from(*expr.clone(), self.input_plan.clone()),
                PyExpr::from(*pattern.clone(), self.input_plan.clone()),
            ]),
            Expr::ILike(Like { expr, pattern, .. }) => Ok(vec![
                PyExpr::from(*expr.clone(), self.input_plan.clone()),
                PyExpr::from(*pattern.clone(), self.input_plan.clone()),
            ]),
            Expr::SimilarTo(Like { expr, pattern, .. }) => Ok(vec![
                PyExpr::from(*expr.clone(), self.input_plan.clone()),
                PyExpr::from(*pattern.clone(), self.input_plan.clone()),
            ]),
            Expr::Between(Between {
                expr,
                negated: _,
                low,
                high,
            }) => Ok(vec![
                PyExpr::from(*expr.clone(), self.input_plan.clone()),
                PyExpr::from(*low.clone(), self.input_plan.clone()),
                PyExpr::from(*high.clone(), self.input_plan.clone()),
            ]),

            // Currently un-support/implemented Expr types for Rex Call operations
            Expr::GroupingSet(..)
            | Expr::Wildcard
            | Expr::QualifiedWildcard { .. }
            | Expr::ScalarSubquery(..)
            | Expr::Placeholder { .. }
            | Expr::Exists { .. } => Err(py_runtime_err(format!(
                "Unimplemented Expr type: {}",
                self.expr
            ))),
        }
    }

    #[pyo3(name = "getOperatorName")]
    pub fn get_operator_name(&self) -> PyResult<String> {
        Ok(match &self.expr {
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op,
                right: _,
            }) => format!("{op}"),
            Expr::ScalarFunction { fun, args: _ } => format!("{fun}"),
            Expr::ScalarUDF { fun, .. } => fun.name.clone(),
            Expr::Cast { .. } => "cast".to_string(),
            Expr::Between { .. } => "between".to_string(),
            Expr::Case { .. } => "case".to_string(),
            Expr::IsNull(..) => "is null".to_string(),
            Expr::IsNotNull(..) => "is not null".to_string(),
            Expr::IsTrue(_) => "is true".to_string(),
            Expr::IsFalse(_) => "is false".to_string(),
            Expr::IsUnknown(_) => "is unknown".to_string(),
            Expr::IsNotTrue(_) => "is not true".to_string(),
            Expr::IsNotFalse(_) => "is not false".to_string(),
            Expr::IsNotUnknown(_) => "is not unknown".to_string(),
            Expr::InList { .. } => "in list".to_string(),
            Expr::Negative(..) => "negative".to_string(),
            Expr::Not(..) => "not".to_string(),
            Expr::Like(Like { negated, .. }) => {
                if *negated {
                    "not like".to_string()
                } else {
                    "like".to_string()
                }
            }
            Expr::ILike(Like { negated, .. }) => {
                if *negated {
                    "not ilike".to_string()
                } else {
                    "ilike".to_string()
                }
            }
            Expr::SimilarTo(Like { negated, .. }) => {
                if *negated {
                    "not similar to".to_string()
                } else {
                    "similar to".to_string()
                }
            }
            _ => {
                return Err(py_type_err(format!(
                    "Catch all triggered in get_operator_name: {:?}",
                    &self.expr
                )))
            }
        })
    }

    /// Gets the ScalarValue represented by the Expression
    #[pyo3(name = "getType")]
    pub fn get_type(&self) -> PyResult<String> {
        Ok(String::from(match &self.expr {
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op,
                right: _,
            }) => match op {
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
                | Operator::And
                | Operator::Or
                | Operator::IsDistinctFrom
                | Operator::IsNotDistinctFrom
                | Operator::RegexMatch
                | Operator::RegexIMatch
                | Operator::RegexNotMatch
                | Operator::RegexNotIMatch => "BOOLEAN",
                Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Modulo => {
                    "BIGINT"
                }
                Operator::Divide => "FLOAT",
                Operator::StringConcat => "VARCHAR",
                Operator::BitwiseShiftLeft
                | Operator::BitwiseShiftRight
                | Operator::BitwiseXor
                | Operator::BitwiseAnd
                | Operator::BitwiseOr => {
                    // the type here should be the same as the type of the left expression
                    // but we can only compute that if we have the schema available
                    return Err(py_type_err(
                        "Bitwise operators unsupported in get_type".to_string(),
                    ));
                }
            },
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Boolean(_value) => "Boolean",
                ScalarValue::Float32(_value) => "Float32",
                ScalarValue::Float64(_value) => "Float64",
                ScalarValue::Decimal128(_value, ..) => "Decimal128",
                ScalarValue::Dictionary(..) => "Dictionary",
                ScalarValue::Int8(_value) => "Int8",
                ScalarValue::Int16(_value) => "Int16",
                ScalarValue::Int32(_value) => "Int32",
                ScalarValue::Int64(_value) => "Int64",
                ScalarValue::UInt8(_value) => "UInt8",
                ScalarValue::UInt16(_value) => "UInt16",
                ScalarValue::UInt32(_value) => "UInt32",
                ScalarValue::UInt64(_value) => "UInt64",
                ScalarValue::Utf8(_value) => "Utf8",
                ScalarValue::LargeUtf8(_value) => "LargeUtf8",
                ScalarValue::Binary(_value) => "Binary",
                ScalarValue::LargeBinary(_value) => "LargeBinary",
                ScalarValue::Date32(_value) => "Date32",
                ScalarValue::Date64(_value) => "Date64",
                ScalarValue::Time32Second(_value) => "Time32",
                ScalarValue::Time32Millisecond(_value) => "Time32",
                ScalarValue::Time64Microsecond(_value) => "Time64",
                ScalarValue::Time64Nanosecond(_value) => "Time64",
                ScalarValue::Null => "Null",
                ScalarValue::TimestampSecond(..) => "TimestampSecond",
                ScalarValue::TimestampMillisecond(..) => "TimestampMillisecond",
                ScalarValue::TimestampMicrosecond(..) => "TimestampMicrosecond",
                ScalarValue::TimestampNanosecond(..) => "TimestampNanosecond",
                ScalarValue::IntervalYearMonth(..) => "IntervalYearMonth",
                ScalarValue::IntervalDayTime(..) => "IntervalDayTime",
                ScalarValue::IntervalMonthDayNano(..) => "IntervalMonthDayNano",
                ScalarValue::List(..) => "List",
                ScalarValue::Struct(..) => "Struct",
                ScalarValue::FixedSizeBinary(_, _) => "FixedSizeBinary",
            },
            Expr::ScalarFunction { fun, args: _ } => match fun {
                BuiltinScalarFunction::Abs => "Abs",
                BuiltinScalarFunction::DatePart => "DatePart",
                _ => {
                    return Err(py_type_err(format!(
                        "Catch all triggered for ScalarFunction in get_type; {fun:?}"
                    )))
                }
            },
            Expr::Cast(Cast { expr: _, data_type }) => match data_type {
                DataType::Null => "NULL",
                DataType::Boolean => "BOOLEAN",
                DataType::Int8 | DataType::UInt8 => "TINYINT",
                DataType::Int16 | DataType::UInt16 => "SMALLINT",
                DataType::Int32 | DataType::UInt32 => "INTEGER",
                DataType::Int64 | DataType::UInt64 => "BIGINT",
                DataType::Float32 => "FLOAT",
                DataType::Float64 => "DOUBLE",
                DataType::Timestamp { .. } => "TIMESTAMP",
                DataType::Date32 | DataType::Date64 => "DATE",
                DataType::Time32(..) => "TIME32",
                DataType::Time64(..) => "TIME64",
                DataType::Duration(..) => "DURATION",
                DataType::Interval(..) => "INTERVAL",
                DataType::Binary => "BINARY",
                DataType::FixedSizeBinary(..) => "FIXEDSIZEBINARY",
                DataType::LargeBinary => "LARGEBINARY",
                DataType::Utf8 => "VARCHAR",
                DataType::LargeUtf8 => "BIGVARCHAR",
                DataType::List(..) => "LIST",
                DataType::FixedSizeList(..) => "FIXEDSIZELIST",
                DataType::LargeList(..) => "LARGELIST",
                DataType::Struct(..) => "STRUCT",
                DataType::Union(..) => "UNION",
                DataType::Dictionary(..) => "DICTIONARY",
                DataType::Decimal128(..) => "DECIMAL",
                DataType::Decimal256(..) => "DECIMAL",
                DataType::Map(..) => "MAP",
                _ => {
                    return Err(py_type_err(format!(
                        "Catch all triggered for Cast in get_type; {data_type:?}"
                    )))
                }
            },
            _ => {
                return Err(py_type_err(format!(
                    "Catch all triggered in get_type; {:?}",
                    &self.expr
                )))
            }
        }))
    }

    #[pyo3(name = "getFilterExpr")]
    pub fn get_filter_expr(&self) -> PyResult<Option<PyExpr>> {
        // TODO refactor to avoid duplication
        match &self.expr {
            Expr::Alias(expr, _) => match expr.as_ref() {
                Expr::AggregateFunction(AggregateFunction { filter, .. })
                | Expr::AggregateUDF { filter, .. } => match filter {
                    Some(filter) => {
                        Ok(Some(PyExpr::from(*filter.clone(), self.input_plan.clone())))
                    }
                    None => Ok(None),
                },
                _ => Err(py_type_err(
                    "getFilterExpr() - Non-aggregate expression encountered",
                )),
            },
            Expr::AggregateFunction(AggregateFunction { filter, .. })
            | Expr::AggregateUDF { filter, .. } => match filter {
                Some(filter) => Ok(Some(PyExpr::from(*filter.clone(), self.input_plan.clone()))),
                None => Ok(None),
            },
            _ => Err(py_type_err(
                "getFilterExpr() - Non-aggregate expression encountered",
            )),
        }
    }

    #[pyo3(name = "getFloat32Value")]
    pub fn float_32_value(&self) -> PyResult<Option<f32>> {
        extract_scalar_value!(self, Float32)
    }

    #[pyo3(name = "getFloat64Value")]
    pub fn float_64_value(&self) -> PyResult<Option<f64>> {
        extract_scalar_value!(self, Float64)
    }

    #[pyo3(name = "getDecimal128Value")]
    pub fn decimal_128_value(&mut self) -> PyResult<(Option<i128>, u8, i8)> {
        match self.get_scalar_value()? {
            ScalarValue::Decimal128(value, precision, scale) => Ok((*value, *precision, *scale)),
            other => Err(unexpected_literal_value(other)),
        }
    }

    #[pyo3(name = "getInt8Value")]
    pub fn int_8_value(&self) -> PyResult<Option<i8>> {
        extract_scalar_value!(self, Int8)
    }

    #[pyo3(name = "getInt16Value")]
    pub fn int_16_value(&self) -> PyResult<Option<i16>> {
        extract_scalar_value!(self, Int16)
    }

    #[pyo3(name = "getInt32Value")]
    pub fn int_32_value(&self) -> PyResult<Option<i32>> {
        extract_scalar_value!(self, Int32)
    }

    #[pyo3(name = "getInt64Value")]
    pub fn int_64_value(&self) -> PyResult<Option<i64>> {
        extract_scalar_value!(self, Int64)
    }

    #[pyo3(name = "getUInt8Value")]
    pub fn uint_8_value(&self) -> PyResult<Option<u8>> {
        extract_scalar_value!(self, UInt8)
    }

    #[pyo3(name = "getUInt16Value")]
    pub fn uint_16_value(&self) -> PyResult<Option<u16>> {
        extract_scalar_value!(self, UInt16)
    }

    #[pyo3(name = "getUInt32Value")]
    pub fn uint_32_value(&self) -> PyResult<Option<u32>> {
        extract_scalar_value!(self, UInt32)
    }

    #[pyo3(name = "getUInt64Value")]
    pub fn uint_64_value(&self) -> PyResult<Option<u64>> {
        extract_scalar_value!(self, UInt64)
    }

    #[pyo3(name = "getDate32Value")]
    pub fn date_32_value(&self) -> PyResult<Option<i32>> {
        extract_scalar_value!(self, Date32)
    }

    #[pyo3(name = "getDate64Value")]
    pub fn date_64_value(&self) -> PyResult<Option<i64>> {
        extract_scalar_value!(self, Date64)
    }

    #[pyo3(name = "getTime64Value")]
    pub fn time_64_value(&self) -> PyResult<Option<i64>> {
        extract_scalar_value!(self, Time64Nanosecond)
    }

    #[pyo3(name = "getTimestampValue")]
    pub fn timestamp_value(&mut self) -> PyResult<(Option<i64>, Option<String>)> {
        match self.get_scalar_value()? {
            ScalarValue::TimestampNanosecond(iv, tz)
            | ScalarValue::TimestampMicrosecond(iv, tz)
            | ScalarValue::TimestampMillisecond(iv, tz)
            | ScalarValue::TimestampSecond(iv, tz) => Ok((*iv, tz.clone())),
            other => Err(unexpected_literal_value(other)),
        }
    }

    #[pyo3(name = "getBoolValue")]
    pub fn bool_value(&self) -> PyResult<Option<bool>> {
        extract_scalar_value!(self, Boolean)
    }

    #[pyo3(name = "getStringValue")]
    pub fn string_value(&self) -> PyResult<Option<String>> {
        match self.get_scalar_value()? {
            ScalarValue::Utf8(value) => Ok(value.clone()),
            other => Err(unexpected_literal_value(other)),
        }
    }

    #[pyo3(name = "getIntervalDayTimeValue")]
    pub fn interval_day_time_value(&self) -> PyResult<Option<(i32, i32)>> {
        match self.get_scalar_value()? {
            ScalarValue::IntervalDayTime(Some(iv)) => {
                let interval = *iv as u64;
                let days = (interval >> 32) as i32;
                let ms = interval as i32;
                Ok(Some((days, ms)))
            }
            ScalarValue::IntervalDayTime(None) => Ok(None),
            other => Err(unexpected_literal_value(other)),
        }
    }

    #[pyo3(name = "isNegated")]
    pub fn is_negated(&self) -> PyResult<bool> {
        match &self.expr {
            Expr::Between(Between { negated, .. })
            | Expr::Exists { negated, .. }
            | Expr::InList { negated, .. }
            | Expr::InSubquery { negated, .. } => Ok(*negated),
            _ => Err(py_type_err(format!(
                "unknown Expr type {:?} encountered",
                &self.expr
            ))),
        }
    }

    #[pyo3(name = "isDistinctAgg")]
    pub fn is_distinct_aggregation(&self) -> PyResult<bool> {
        // TODO refactor to avoid duplication
        match &self.expr {
            Expr::AggregateFunction(funct) => Ok(funct.distinct),
            Expr::AggregateUDF { .. } => Ok(false),
            Expr::Alias(expr, _) => match expr.as_ref() {
                Expr::AggregateFunction(funct) => Ok(funct.distinct),
                Expr::AggregateUDF { .. } => Ok(false),
                _ => Err(py_type_err(
                    "isDistinctAgg() - Non-aggregate expression encountered",
                )),
            },
            _ => Err(py_type_err(
                "getFilterExpr() - Non-aggregate expression encountered",
            )),
        }
    }

    /// Returns if a sort expressions is an ascending sort
    #[pyo3(name = "isSortAscending")]
    pub fn is_sort_ascending(&self) -> PyResult<bool> {
        match &self.expr {
            Expr::Sort(Sort { asc, .. }) => Ok(*asc),
            _ => Err(py_type_err(format!(
                "Provided Expr {:?} is not a sort type",
                &self.expr
            ))),
        }
    }

    /// Returns if nulls should be placed first in a sort expression
    #[pyo3(name = "isSortNullsFirst")]
    pub fn is_sort_nulls_first(&self) -> PyResult<bool> {
        match &self.expr {
            Expr::Sort(Sort { nulls_first, .. }) => Ok(*nulls_first),
            _ => Err(py_type_err(format!(
                "Provided Expr {:?} is not a sort type",
                &self.expr
            ))),
        }
    }

    /// Returns the escape char for like/ilike/similar to expr variants
    #[pyo3(name = "getEscapeChar")]
    pub fn get_escape_char(&self) -> PyResult<Option<char>> {
        match &self.expr {
            Expr::Like(Like { escape_char, .. })
            | Expr::ILike(Like { escape_char, .. })
            | Expr::SimilarTo(Like { escape_char, .. }) => Ok(*escape_char),
            _ => Err(py_type_err(format!(
                "Provided Expr {:?} not one of Like/ILike/SimilarTo",
                &self.expr
            ))),
        }
    }
}

impl PyExpr {
    /// Get the scalar value represented by this literal expression, returning an error
    /// if this is not a literal expression
    fn get_scalar_value(&self) -> Result<&ScalarValue> {
        match &self.expr {
            Expr::Literal(v) => Ok(v),
            _ => Err(DaskPlannerError::Internal(
                "get_scalar_value() called on non-literal expression".to_string(),
            )),
        }
    }
}

fn unexpected_literal_value(value: &ScalarValue) -> PyErr {
    DaskPlannerError::Internal(format!("getValue<T>() - Unexpected value: {value}")).into()
}

fn get_expr_name(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Alias(expr, _) => get_expr_name(expr),
        _ => Ok(expr.canonical_name()),
    }
}

/// Create a [DFField] representing an [Expr], given an input [LogicalPlan] to resolve against
pub fn expr_to_field(expr: &Expr, input_plan: &LogicalPlan) -> Result<DFField> {
    match expr {
        Expr::Sort(Sort { expr, .. }) => {
            // DataFusion does not support create_name for sort expressions (since they never
            // appear in projections) so we just delegate to the contained expression instead
            expr_to_field(expr, input_plan)
        }
        _ => {
            let fields =
                exprlist_to_fields(&[expr.clone()], input_plan).map_err(DaskPlannerError::from)?;
            Ok(fields[0].clone())
        }
    }
}

#[cfg(test)]
mod test {
    use datafusion_common::{Column, ScalarValue};
    use datafusion_expr::Expr;

    use crate::{error::Result, expression::PyExpr};

    #[test]
    fn get_value_u32() -> Result<()> {
        test_get_value(ScalarValue::UInt32(None))?;
        test_get_value(ScalarValue::UInt32(Some(123)))
    }

    #[test]
    fn get_value_utf8() -> Result<()> {
        test_get_value(ScalarValue::Utf8(None))?;
        test_get_value(ScalarValue::Utf8(Some("hello".to_string())))
    }

    #[test]
    fn get_value_non_literal() -> Result<()> {
        let expr = PyExpr::from(Expr::Column(Column::from_qualified_name("a.b")), None);
        let error = expr
            .get_scalar_value()
            .expect_err("cannot get scalar value from column");
        assert_eq!(
            "Internal(\"get_scalar_value() called on non-literal expression\")",
            &format!("{:?}", error)
        );
        Ok(())
    }

    fn test_get_value(value: ScalarValue) -> Result<()> {
        let expr = PyExpr::from(Expr::Literal(value.clone()), None);
        assert_eq!(&value, expr.get_scalar_value()?);
        Ok(())
    }
}
