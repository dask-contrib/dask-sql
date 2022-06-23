use crate::sql::exceptions::py_runtime_err;
use crate::sql::logical;
use crate::sql::types::RexType;
use arrow::datatypes::DataType;
use datafusion_common::{Column, DFField, DFSchema, Result, ScalarValue};
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
            _ => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
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
                        Ok(fq_name) => Ok(input_plans[0]
                            .schema()
                            .index_of_column(&Column::from_qualified_name(&fq_name))
                            .unwrap()),
                        Err(e) => panic!("{:?}", e),
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
                                    panic!("Unable to find match for column with name: '{}' in DFSchema", &fq_name);
                                }
                            }
                        }
                        Err(e) => panic!("{:?}", e),
                    }
                } else {
                    panic!("Not really sure what we should do right here???");
                }
            }
            None => {
                panic!("We need a valid LogicalPlan instance to get the Expr's index in the schema")
            }
        }
    }

    /// Examine the current/"self" PyExpr and return its "type"
    /// In this context a "type" is what Dask-SQL Python
    /// RexConverter plugin instance should be invoked to handle
    /// the Rex conversion
    #[pyo3(name = "getExprType")]
    pub fn get_expr_type(&self) -> String {
        String::from(match &self.expr {
            Expr::Alias(..) => "Alias",
            Expr::Column(..) => "Column",
            Expr::ScalarVariable(..) => panic!("ScalarVariable!!!"),
            Expr::Literal(..) => "Literal",
            Expr::BinaryExpr { .. } => "BinaryExpr",
            Expr::Not(..) => panic!("Not!!!"),
            Expr::IsNotNull(..) => panic!("IsNotNull!!!"),
            Expr::Negative(..) => panic!("Negative!!!"),
            Expr::GetIndexedField { .. } => panic!("GetIndexedField!!!"),
            Expr::IsNull(..) => panic!("IsNull!!!"),
            Expr::Between { .. } => panic!("Between!!!"),
            Expr::Case { .. } => panic!("Case!!!"),
            Expr::Cast { .. } => "Cast",
            Expr::TryCast { .. } => panic!("TryCast!!!"),
            Expr::Sort { .. } => "Sort",
            Expr::ScalarFunction { .. } => "ScalarFunction",
            Expr::AggregateFunction { .. } => "AggregateFunction",
            Expr::WindowFunction { .. } => panic!("WindowFunction!!!"),
            Expr::AggregateUDF { .. } => panic!("AggregateUDF!!!"),
            Expr::InList { .. } => panic!("InList!!!"),
            Expr::Wildcard => panic!("Wildcard!!!"),
            Expr::InSubquery { .. } => "Subquery",
            Expr::ScalarUDF { .. } => "ScalarUDF",
            Expr::Exists { .. } => "Exists",
            Expr::ScalarSubquery(..) => "ScalarSubquery",
            Expr::QualifiedWildcard { .. } => "Wildcard",
            Expr::GroupingSet(..) => "GroupingSet",
        })
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

    /// Gets the operands for a BinaryExpr call
    #[pyo3(name = "getOperands")]
    pub fn get_operands(&self) -> PyResult<Vec<PyExpr>> {
        match &self.expr {
            Expr::BinaryExpr { left, right, .. } => Ok(vec![
                PyExpr::from(*left.clone(), self.input_plan.clone()),
                PyExpr::from(*right.clone(), self.input_plan.clone()),
            ]),
            Expr::ScalarFunction { fun: _, args } => {
                let mut operands: Vec<PyExpr> = Vec::new();
                for arg in args {
                    operands.push(PyExpr::from(arg.clone(), self.input_plan.clone()));
                }
                Ok(operands)
            }
            Expr::Cast { expr, data_type: _ } => {
                Ok(vec![PyExpr::from(*expr.clone(), self.input_plan.clone())])
            }
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
            Expr::InSubquery {
                expr: _,
                subquery: _,
                negated: _,
            } => {
                unimplemented!("InSubquery")
            }
            Expr::ScalarSubquery(subquery) => {
                let _plan = &subquery.subquery;
                unimplemented!("ScalarSubquery")
            }
            Expr::IsNotNull(expr) => Ok(vec![PyExpr::from(*expr.clone(), self.input_plan.clone())]),
            Expr::ScalarUDF { args, .. } => Ok(args
                .iter()
                .map(|arg| PyExpr::from(arg.clone(), self.input_plan.clone()))
                .collect()),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "unknown Expr type {:?} encountered",
                &self.expr
            ))),
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
            Expr::IsNotNull(..) => Ok("is not null".to_string()),
            Expr::ScalarUDF { fun, .. } => Ok(fun.name.clone()),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "Catch all triggered for get_operator_name: {:?}",
                &self.expr
            ))),
        }
    }

    /// Gets the ScalarValue represented by the Expression
    #[pyo3(name = "getType")]
    pub fn get_type(&self) -> PyResult<String> {
        match &self.expr {
            Expr::ScalarVariable(..) => panic!("ScalarVariable!!!"),
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Boolean(_value) => Ok(String::from("Boolean")),
                ScalarValue::Float32(_value) => Ok(String::from("Float32")),
                ScalarValue::Float64(_value) => Ok(String::from("Float64")),
                ScalarValue::Decimal128(_value, ..) => Ok(String::from("Decimal128")),
                ScalarValue::Int8(_value) => Ok(String::from("Int8")),
                ScalarValue::Int16(_value) => Ok(String::from("Int16")),
                ScalarValue::Int32(_value) => Ok(String::from("Int32")),
                ScalarValue::Int64(_value) => Ok(String::from("Int64")),
                ScalarValue::UInt8(_value) => Ok(String::from("UInt8")),
                ScalarValue::UInt16(_value) => Ok(String::from("UInt16")),
                ScalarValue::UInt32(_value) => Ok(String::from("UInt32")),
                ScalarValue::UInt64(_value) => Ok(String::from("UInt64")),
                ScalarValue::Utf8(_value) => Ok(String::from("Utf8")),
                ScalarValue::LargeUtf8(_value) => Ok(String::from("LargeUtf8")),
                ScalarValue::Binary(_value) => Ok(String::from("Binary")),
                ScalarValue::LargeBinary(_value) => Ok(String::from("LargeBinary")),
                ScalarValue::Date32(_value) => Ok(String::from("Date32")),
                ScalarValue::Date64(_value) => Ok(String::from("Date64")),
                _ => {
                    panic!("CatchAll")
                }
            },
            Expr::ScalarFunction { fun, args: _ } => match fun {
                BuiltinScalarFunction::Abs => Ok(String::from("Abs")),
                BuiltinScalarFunction::DatePart => Ok(String::from("DatePart")),
                _ => {
                    panic!("fire here for scalar function")
                }
            },
            Expr::Cast { expr: _, data_type } => match data_type {
                DataType::Null => Ok(String::from("NULL")),
                DataType::Boolean => Ok(String::from("BOOLEAN")),
                DataType::Int8 => Ok(String::from("TINYINT")),
                DataType::UInt8 => Ok(String::from("TINYINT")),
                DataType::Int16 => Ok(String::from("SMALLINT")),
                DataType::UInt16 => Ok(String::from("SMALLINT")),
                DataType::Int32 => Ok(String::from("INTEGER")),
                DataType::UInt32 => Ok(String::from("INTEGER")),
                DataType::Int64 => Ok(String::from("BIGINT")),
                DataType::UInt64 => Ok(String::from("BIGINT")),
                DataType::Float32 => Ok(String::from("FLOAT")),
                DataType::Float64 => Ok(String::from("DOUBLE")),
                DataType::Timestamp { .. } => Ok(String::from("TIMESTAMP")),
                DataType::Date32 => Ok(String::from("DATE")),
                DataType::Date64 => Ok(String::from("DATE")),
                DataType::Time32(..) => Ok(String::from("TIME32")),
                DataType::Time64(..) => Ok(String::from("TIME64")),
                DataType::Duration(..) => Ok(String::from("DURATION")),
                DataType::Interval(..) => Ok(String::from("INTERVAL")),
                DataType::Binary => Ok(String::from("BINARY")),
                DataType::FixedSizeBinary(..) => Ok(String::from("FIXEDSIZEBINARY")),
                DataType::LargeBinary => Ok(String::from("LARGEBINARY")),
                DataType::Utf8 => Ok(String::from("VARCHAR")),
                DataType::LargeUtf8 => Ok(String::from("BIGVARCHAR")),
                DataType::List(..) => Ok(String::from("LIST")),
                DataType::FixedSizeList(..) => Ok(String::from("FIXEDSIZELIST")),
                DataType::LargeList(..) => Ok(String::from("LARGELIST")),
                DataType::Struct(..) => Ok(String::from("STRUCT")),
                DataType::Union(..) => Ok(String::from("UNION")),
                DataType::Dictionary(..) => Ok(String::from("DICTIONARY")),
                DataType::Decimal(..) => Ok(String::from("DECIMAL")),
                DataType::Map(..) => Ok(String::from("MAP")),
                _ => {
                    panic!("This is not yet implemented!!!")
                }
            },
            _ => panic!("OTHER"),
        }
    }

    /// TODO: I can't express how much I dislike explicity listing all of these methods out
    /// but PyO3 makes it necessary since its annotations cannot be used in trait impl blocks
    #[pyo3(name = "getFloat32Value")]
    pub fn float_32_value(&mut self) -> f32 {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Float32(iv) => iv.unwrap(),
                _ => {
                    panic!("getValue<T>() - Unexpected value")
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered"),
        }
    }

    #[pyo3(name = "getFloat64Value")]
    pub fn float_64_value(&mut self) -> f64 {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Float64(iv) => iv.unwrap(),
                _ => {
                    panic!("getValue<T>() - Unexpected value")
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered"),
        }
    }

    #[pyo3(name = "getInt8Value")]
    pub fn int_8_value(&mut self) -> i8 {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Int8(iv) => iv.unwrap(),
                _ => {
                    panic!("getValue<T>() - Unexpected value")
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered"),
        }
    }

    #[pyo3(name = "getInt16Value")]
    pub fn int_16_value(&mut self) -> i16 {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Int16(iv) => iv.unwrap(),
                _ => {
                    panic!("getValue<T>() - Unexpected value")
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered"),
        }
    }

    #[pyo3(name = "getInt32Value")]
    pub fn int_32_value(&mut self) -> i32 {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Int32(iv) => iv.unwrap(),
                _ => {
                    panic!("getValue<T>() - Unexpected value")
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered"),
        }
    }

    #[pyo3(name = "getInt64Value")]
    pub fn int_64_value(&mut self) -> i64 {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Int64(iv) => iv.unwrap(),
                _ => {
                    panic!("getValue<T>() - Unexpected value")
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered"),
        }
    }

    #[pyo3(name = "getUInt8Value")]
    pub fn uint_8_value(&mut self) -> u8 {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::UInt8(iv) => iv.unwrap(),
                _ => {
                    panic!("getValue<T>() - Unexpected value")
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered"),
        }
    }

    #[pyo3(name = "getUInt16Value")]
    pub fn uint_16_value(&mut self) -> u16 {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::UInt16(iv) => iv.unwrap(),
                _ => {
                    panic!("getValue<T>() - Unexpected value")
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered"),
        }
    }

    #[pyo3(name = "getUInt32Value")]
    pub fn uint_32_value(&mut self) -> u32 {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::UInt32(iv) => iv.unwrap(),
                _ => {
                    panic!("getValue<T>() - Unexpected value")
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered"),
        }
    }

    #[pyo3(name = "getUInt64Value")]
    pub fn uint_64_value(&mut self) -> u64 {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::UInt64(iv) => iv.unwrap(),
                _ => {
                    panic!("getValue<T>() - Unexpected value")
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered"),
        }
    }

    #[pyo3(name = "getBoolValue")]
    pub fn bool_value(&mut self) -> bool {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Boolean(iv) => iv.unwrap(),
                _ => {
                    panic!("getValue<T>() - Unexpected value")
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered"),
        }
    }

    #[pyo3(name = "getStringValue")]
    pub fn string_value(&mut self) -> String {
        match &self.expr {
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Utf8(iv) => iv.clone().unwrap(),
                _ => {
                    panic!("getValue<T>() - Unexpected value")
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered"),
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
