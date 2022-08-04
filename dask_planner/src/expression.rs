use crate::sql::exceptions::{py_runtime_err, py_type_err};
use crate::sql::logical;
use crate::sql::types::RexType;
use arrow::datatypes::DataType;
use datafusion_common::{Column, DFField, DFSchema, Result, ScalarValue};
use datafusion_expr::Operator;
use datafusion_expr::{lit, utils::exprlist_to_fields, BuiltinScalarFunction, Expr, LogicalPlan};
use pyo3::prelude::*;
use std::convert::From;
use std::sync::Arc;

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
            Expr::Alias(..) => RexType::Reference,
            Expr::Column(..) => RexType::Reference,
            Expr::ScalarVariable(..) => RexType::Literal,
            Expr::Literal(..) => RexType::Literal,
            Expr::BinaryExpr { .. } => RexType::Call,
            Expr::Not(..) => RexType::Call,
            Expr::IsNotNull(..) => RexType::Call,
            Expr::Negative(..) => RexType::Call,
            Expr::GetIndexedField { .. } => RexType::Reference,
            Expr::IsNull(..) => RexType::Call,
            Expr::Between { .. } => RexType::Call,
            Expr::Case { .. } => RexType::Call,
            Expr::Cast { .. } => RexType::Call,
            Expr::TryCast { .. } => RexType::Call,
            Expr::Sort { .. } => RexType::Call,
            Expr::ScalarFunction { .. } => RexType::Call,
            Expr::AggregateFunction { .. } => RexType::Call,
            Expr::WindowFunction { .. } => RexType::Call,
            Expr::AggregateUDF { .. } => RexType::Call,
            Expr::InList { .. } => RexType::Call,
            Expr::Wildcard => RexType::Call,
            Expr::ScalarUDF { .. } => RexType::Call,
            Expr::Exists { .. } => RexType::Call,
            Expr::InSubquery { .. } => RexType::Call,
            Expr::ScalarSubquery(..) => RexType::SubqueryAlias,
            Expr::QualifiedWildcard { .. } => RexType::Reference,
            Expr::GroupingSet(..) => RexType::Call,
        }
    }
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
            Expr::ScalarSubquery(subquery) => Ok((&*subquery.subquery).clone().into()),
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
        match &self.expr {
            Expr::Column(_col) => Ok(true),
            _ => Ok(false),
        }
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
            Some(input_plans) => {
                if input_plans.len() == 1 {
                    let name: Result<String> = self.expr.name(input_plans[0].schema());
                    match name {
                        Ok(fq_name) => {
                            let mut idx: usize = 0;
                            for schema in input_plans[0].all_schemas() {
                                match schema.index_of_column(&Column::from_qualified_name(&fq_name))
                                {
                                    Ok(e) => {
                                        idx = e;
                                        break;
                                    }
                                    Err(_e) => (),
                                }
                            }
                            Ok(idx)
                        }
                        Err(e) => Err(py_runtime_err(e)),
                    }
                } else if input_plans.len() >= 2 {
                    let mut base_schema: DFSchema = (**input_plans[0].schema()).clone();
                    for input_idx in 1..input_plans.len() {
                        let input_schema: DFSchema = (**input_plans[input_idx].schema()).clone();
                        base_schema.merge(&input_schema);
                    }
                    let name: Result<String> = self.expr.name(&base_schema);
                    match name {
                        Ok(fq_name) => {
                            let idx: Result<usize> =
                                base_schema.index_of_column(&Column::from_qualified_name(&fq_name));
                            match idx {
                                Ok(index) => Ok(index),
                                Err(_) => {
                                    // This logic is encountered when an non-qualified column name is
                                    // provided AND there exists more than one entry with that
                                    // unqualified. This logic will attempt to narrow down to the
                                    // qualified column name.
                                    let qualified_fields: Vec<&DFField> =
                                        base_schema.fields_with_unqualified_name(&fq_name);
                                    for qf in &qualified_fields {
                                        if qf.name().eq(&fq_name) {
                                            let qualifier: String = qf.qualifier().unwrap().clone();
                                            let qual: Option<&str> = Some(&qualifier);
                                            let index: usize = base_schema
                                                .index_of_column_by_name(qual, qf.name())
                                                .unwrap();
                                            return Ok(index);
                                        }
                                    }
                                    Err(py_runtime_err(format!("Unable to find match for column with name: '{}' in DFSchema", &fq_name)))
                                }
                            }
                        }
                        Err(e) => Err(py_runtime_err(e)),
                    }
                } else {
                    Err(py_runtime_err(
                        "Not really sure what we should do right here???",
                    ))
                }
            }
            None => Err(py_runtime_err(
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
        match &self.expr {
            Expr::Alias(..) => Ok("Alias".to_string()),
            Expr::Column(..) => Ok("Column".to_string()),
            Expr::ScalarVariable(..) => Err(py_type_err("ScalarVariable!!!")),
            Expr::Literal(..) => Ok("Literal".to_string()),
            Expr::BinaryExpr { .. } => Ok("BinaryExpr".to_string()),
            Expr::Not(..) => Err(py_type_err("Not!!!")),
            Expr::IsNotNull(..) => Err(py_type_err("IsNotNull!!!")),
            Expr::Negative(..) => Err(py_type_err("Negative!!!")),
            Expr::GetIndexedField { .. } => Err(py_type_err("GetIndexedField!!!")),
            Expr::IsNull(..) => Err(py_type_err("IsNull!!!")),
            Expr::Between { .. } => Ok("Between".to_string()),
            Expr::Case { .. } => Err(py_type_err("Case!!!")),
            Expr::Cast { .. } => Ok("Cast".to_string()),
            Expr::TryCast { .. } => Err(py_type_err("TryCast!!!")),
            Expr::Sort { .. } => Ok("Sort".to_string()),
            Expr::ScalarFunction { .. } => Ok("ScalarFunction".to_string()),
            Expr::AggregateFunction { .. } => Ok("AggregateFunction".to_string()),
            Expr::WindowFunction { .. } => Err(py_type_err("WindowFunction!!!")),
            Expr::AggregateUDF { .. } => Err(py_type_err("AggregateUDF!!!")),
            Expr::InList { .. } => Ok("InList".to_string()),
            Expr::Wildcard => Err(py_type_err("Wildcard!!!")),
            Expr::InSubquery { .. } => Ok("Subquery".to_string()),
            Expr::ScalarUDF { .. } => Ok("ScalarUDF".to_string()),
            Expr::Exists { .. } => Ok("Exists".to_string()),
            Expr::ScalarSubquery(..) => Ok("ScalarSubquery".to_string()),
            Expr::QualifiedWildcard { .. } => Ok("Wildcard".to_string()),
            Expr::GroupingSet(..) => Ok("GroupingSet".to_string()),
        }
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
            | Expr::Negative(expr)
            | Expr::GetIndexedField { expr, .. }
            | Expr::Cast { expr, .. }
            | Expr::TryCast { expr, .. }
            | Expr::Sort { expr, .. }
            | Expr::InSubquery { expr, .. } => {
                Ok(vec![PyExpr::from(*expr.clone(), self.input_plan.clone())])
            }

            // Expr variants containing a collection of Expr(s) for operands
            Expr::AggregateFunction { args, .. }
            | Expr::AggregateUDF { args, .. }
            | Expr::ScalarFunction { args, .. }
            | Expr::ScalarUDF { args, .. }
            | Expr::WindowFunction { args, .. } => Ok(args
                .iter()
                .map(|arg| PyExpr::from(arg.clone(), self.input_plan.clone()))
                .collect()),

            // Expr(s) that require more specific processing
            Expr::Case {
                expr,
                when_then_expr,
                else_expr,
            } => {
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
                let mut operands: Vec<PyExpr> = Vec::new();
                operands.push(PyExpr::from(*expr.clone(), self.input_plan.clone()));
                for list_elem in list {
                    operands.push(PyExpr::from(list_elem.clone(), self.input_plan.clone()));
                }

                Ok(operands)
            }
            Expr::BinaryExpr { left, right, .. } => Ok(vec![
                PyExpr::from(*left.clone(), self.input_plan.clone()),
                PyExpr::from(*right.clone(), self.input_plan.clone()),
            ]),
            Expr::Between {
                expr,
                negated: _,
                low,
                high,
            } => Ok(vec![
                PyExpr::from(*expr.clone(), self.input_plan.clone()),
                PyExpr::from(*low.clone(), self.input_plan.clone()),
                PyExpr::from(*high.clone(), self.input_plan.clone()),
            ]),

            // Currently un-support/implemented Expr types for Rex Call operations
            Expr::GroupingSet(..)
            | Expr::Wildcard
            | Expr::QualifiedWildcard { .. }
            | Expr::ScalarSubquery(..)
            | Expr::Exists { .. } => unimplemented!("Unimplmented Expr type"),
        }
    }

    #[pyo3(name = "getOperatorName")]
    pub fn get_operator_name(&self) -> PyResult<String> {
        match &self.expr {
            Expr::BinaryExpr {
                left: _,
                op,
                right: _,
            } => Ok(format!("{}", op)),
            Expr::ScalarFunction { fun, args: _ } => Ok(format!("{}", fun)),
            Expr::Cast { .. } => Ok("cast".to_string()),
            Expr::Between { .. } => Ok("between".to_string()),
            Expr::Case { .. } => Ok("case".to_string()),
            Expr::IsNull(..) => Ok("is null".to_string()),
            Expr::IsNotNull(..) => Ok("is not null".to_string()),
            Expr::ScalarUDF { fun, .. } => Ok(fun.name.clone()),
            Expr::InList { .. } => Ok("in list".to_string()),
            Expr::Negative(..) => Ok("negative".to_string()),
            _ => Err(py_type_err(format!(
                "Catch all triggered for get_operator_name: {:?}",
                &self.expr
            ))),
        }
    }

    /// Gets the ScalarValue represented by the Expression
    #[pyo3(name = "getType")]
    pub fn get_type(&self) -> PyResult<String> {
        match &self.expr {
            Expr::BinaryExpr {
                left: _,
                op,
                right: _,
            } => match op {
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
                | Operator::And
                | Operator::Or
                | Operator::Like
                | Operator::NotLike
                | Operator::IsDistinctFrom
                | Operator::IsNotDistinctFrom
                | Operator::RegexMatch
                | Operator::RegexIMatch
                | Operator::RegexNotMatch
                | Operator::RegexNotIMatch
                | Operator::BitwiseAnd
                | Operator::BitwiseOr => Ok("BOOLEAN".to_string()),
                Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Modulo => {
                    Ok("BIGINT".to_string())
                }
                Operator::Divide => Ok("FLOAT".to_string()),
                Operator::StringConcat => Ok("VARCHAR".to_string()),
            },
            Expr::ScalarVariable(..) => Err(py_type_err("ScalarVariable!!!")),
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Boolean(_value) => Ok("Boolean".to_string()),
                ScalarValue::Float32(_value) => Ok("Float32".to_string()),
                ScalarValue::Float64(_value) => Ok("Float64".to_string()),
                ScalarValue::Decimal128(_value, ..) => Ok("Decimal128".to_string()),
                ScalarValue::Int8(_value) => Ok("Int8".to_string()),
                ScalarValue::Int16(_value) => Ok("Int16".to_string()),
                ScalarValue::Int32(_value) => Ok("Int32".to_string()),
                ScalarValue::Int64(_value) => Ok("Int64".to_string()),
                ScalarValue::UInt8(_value) => Ok("UInt8".to_string()),
                ScalarValue::UInt16(_value) => Ok("UInt16".to_string()),
                ScalarValue::UInt32(_value) => Ok("UInt32".to_string()),
                ScalarValue::UInt64(_value) => Ok("UInt64".to_string()),
                ScalarValue::Utf8(_value) => Ok("Utf8".to_string()),
                ScalarValue::LargeUtf8(_value) => Ok("LargeUtf8".to_string()),
                ScalarValue::Binary(_value) => Ok("Binary".to_string()),
                ScalarValue::LargeBinary(_value) => Ok("LargeBinary".to_string()),
                ScalarValue::Date32(_value) => Ok("Date32".to_string()),
                ScalarValue::Date64(_value) => Ok("Date64".to_string()),
                ScalarValue::Null => Ok("Null".to_string()),
                _ => Err(py_type_err("CatchAll")),
            },
            Expr::ScalarFunction { fun, args: _ } => match fun {
                BuiltinScalarFunction::Abs => Ok("Abs".to_string()),
                BuiltinScalarFunction::DatePart => Ok("DatePart".to_string()),
                _ => Err(py_type_err("fire here for scalar function")),
            },
            Expr::Cast { expr: _, data_type } => match data_type {
                DataType::Null => Ok("NULL".to_string()),
                DataType::Boolean => Ok("BOOLEAN".to_string()),
                DataType::Int8 => Ok("TINYINT".to_string()),
                DataType::UInt8 => Ok("TINYINT".to_string()),
                DataType::Int16 => Ok("SMALLINT".to_string()),
                DataType::UInt16 => Ok("SMALLINT".to_string()),
                DataType::Int32 => Ok("INTEGER".to_string()),
                DataType::UInt32 => Ok("INTEGER".to_string()),
                DataType::Int64 => Ok("BIGINT".to_string()),
                DataType::UInt64 => Ok("BIGINT".to_string()),
                DataType::Float32 => Ok("FLOAT".to_string()),
                DataType::Float64 => Ok("DOUBLE".to_string()),
                DataType::Timestamp { .. } => Ok("TIMESTAMP".to_string()),
                DataType::Date32 => Ok("DATE".to_string()),
                DataType::Date64 => Ok("DATE".to_string()),
                DataType::Time32(..) => Ok("TIME32".to_string()),
                DataType::Time64(..) => Ok("TIME64".to_string()),
                DataType::Duration(..) => Ok("DURATION".to_string()),
                DataType::Interval(..) => Ok("INTERVAL".to_string()),
                DataType::Binary => Ok("BINARY".to_string()),
                DataType::FixedSizeBinary(..) => Ok("FIXEDSIZEBINARY".to_string()),
                DataType::LargeBinary => Ok("LARGEBINARY".to_string()),
                DataType::Utf8 => Ok("VARCHAR".to_string()),
                DataType::LargeUtf8 => Ok("BIGVARCHAR".to_string()),
                DataType::List(..) => Ok("LIST".to_string()),
                DataType::FixedSizeList(..) => Ok("FIXEDSIZELIST".to_string()),
                DataType::LargeList(..) => Ok("LARGELIST".to_string()),
                DataType::Struct(..) => Ok("STRUCT".to_string()),
                DataType::Union(..) => Ok("UNION".to_string()),
                DataType::Dictionary(..) => Ok("DICTIONARY".to_string()),
                DataType::Decimal(..) => Ok("DECIMAL".to_string()),
                DataType::Map(..) => Ok("MAP".to_string()),
                _ => Err(py_type_err("This is not yet implemented!!!")),
            },
            _ => Err(py_type_err("OTHER")),
        }
    }

    /// TODO: I can't express how much I dislike explicity listing all of these methods out
    /// but PyO3 makes it necessary since its annotations cannot be used in trait impl blocks
    #[pyo3(name = "getFloat32Value")]
    pub fn float_32_value(&mut self) -> PyResult<f32> {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Float32(iv) => Ok(iv.unwrap()),
                _ => Err(py_type_err("getValue<T>() - Unexpected value")),
            },
            _ => Err(py_type_err("getValue<T>() - Non literal value encountered")),
        }
    }

    #[pyo3(name = "getFloat64Value")]
    pub fn float_64_value(&mut self) -> PyResult<f64> {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Float64(iv) => Ok(iv.unwrap()),
                _ => Err(py_type_err("getValue<T>() - Unexpected value")),
            },
            _ => Err(py_type_err("getValue<T>() - Non literal value encountered")),
        }
    }

    #[pyo3(name = "getInt8Value")]
    pub fn int_8_value(&mut self) -> PyResult<i8> {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Int8(iv) => Ok(iv.unwrap()),
                _ => Err(py_type_err("getValue<T>() - Unexpected value")),
            },
            _ => Err(py_type_err("getValue<T>() - Non literal value encountered")),
        }
    }

    #[pyo3(name = "getInt16Value")]
    pub fn int_16_value(&mut self) -> PyResult<i16> {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Int16(iv) => Ok(iv.unwrap()),
                _ => Err(py_type_err("getValue<T>() - Unexpected value")),
            },
            _ => Err(py_type_err("getValue<T>() - Non literal value encountered")),
        }
    }

    #[pyo3(name = "getInt32Value")]
    pub fn int_32_value(&mut self) -> PyResult<i32> {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Int32(iv) => Ok(iv.unwrap()),
                _ => Err(py_type_err("getValue<T>() - Unexpected value")),
            },
            _ => Err(py_type_err("getValue<T>() - Non literal value encountered")),
        }
    }

    #[pyo3(name = "getInt64Value")]
    pub fn int_64_value(&mut self) -> PyResult<i64> {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Int64(iv) => Ok(iv.unwrap()),
                _ => Err(py_type_err("getValue<T>() - Unexpected value")),
            },
            _ => Err(py_type_err("getValue<T>() - Non literal value encountered")),
        }
    }

    #[pyo3(name = "getUInt8Value")]
    pub fn uint_8_value(&mut self) -> PyResult<u8> {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::UInt8(iv) => Ok(iv.unwrap()),
                _ => Err(py_type_err("getValue<T>() - Unexpected value")),
            },
            _ => Err(py_type_err("getValue<T>() - Non literal value encountered")),
        }
    }

    #[pyo3(name = "getUInt16Value")]
    pub fn uint_16_value(&mut self) -> PyResult<u16> {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::UInt16(iv) => Ok(iv.unwrap()),
                _ => Err(py_type_err("getValue<T>() - Unexpected value")),
            },
            _ => Err(py_type_err("getValue<T>() - Non literal value encountered")),
        }
    }

    #[pyo3(name = "getUInt32Value")]
    pub fn uint_32_value(&mut self) -> PyResult<u32> {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::UInt32(iv) => Ok(iv.unwrap()),
                _ => Err(py_type_err("getValue<T>() - Unexpected value")),
            },
            _ => Err(py_type_err("getValue<T>() - Non literal value encountered")),
        }
    }

    #[pyo3(name = "getUInt64Value")]
    pub fn uint_64_value(&mut self) -> PyResult<u64> {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::UInt64(iv) => Ok(iv.unwrap()),
                _ => Err(py_type_err("getValue<T>() - Unexpected value")),
            },
            _ => Err(py_type_err("getValue<T>() - Non literal value encountered")),
        }
    }

    #[pyo3(name = "getBoolValue")]
    pub fn bool_value(&mut self) -> PyResult<bool> {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Boolean(iv) => Ok(iv.unwrap()),
                _ => Err(py_type_err("getValue<T>() - Unexpected value")),
            },
            _ => Err(py_type_err("getValue<T>() - Non literal value encountered")),
        }
    }

    #[pyo3(name = "getStringValue")]
    pub fn string_value(&mut self) -> PyResult<String> {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Utf8(iv) => Ok(iv.clone().unwrap()),
                _ => Err(py_type_err("getValue<T>() - Unexpected value")),
            },
            _ => Err(py_type_err("getValue<T>() - Non literal value encountered")),
        }
    }

    #[pyo3(name = "isNegated")]
    pub fn is_negated(&self) -> PyResult<bool> {
        match &self.expr {
            Expr::Between { negated, .. }
            | Expr::Exists { negated, .. }
            | Expr::InList { negated, .. }
            | Expr::InSubquery { negated, .. } => Ok(negated.clone()),
            _ => Err(py_type_err(format!(
                "unknown Expr type {:?} encountered",
                &self.expr
            ))),
        }
    }

    /// Returns if a sort expressions is an ascending sort
    #[pyo3(name = "isSortAscending")]
    pub fn is_sort_ascending(&self) -> PyResult<bool> {
        match &self.expr {
            Expr::Sort { asc, .. } => Ok(*asc),
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
            Expr::Sort { nulls_first, .. } => Ok(*nulls_first),
            _ => Err(py_type_err(format!(
                "Provided Expr {:?} is not a sort type",
                &self.expr
            ))),
        }
    }
}

/// Create a [DFField] representing an [Expr], given an input [LogicalPlan] to resolve against
pub fn expr_to_field(expr: &Expr, input_plan: &LogicalPlan) -> Result<DFField> {
    match expr {
        Expr::Sort { expr, .. } => {
            // DataFusion does not support create_name for sort expressions (since they never
            // appear in projections) so we just delegate to the contained expression instead
            expr_to_field(expr, input_plan)
        }
        _ => {
            let fields = exprlist_to_fields(&[expr.clone()], input_plan)?;
            Ok(fields[0].clone())
        }
    }
}
