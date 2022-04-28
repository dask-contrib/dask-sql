use crate::sql::logical;
use crate::sql::types::RexType;

use pyo3::prelude::*;
use std::convert::{From, Into};

use datafusion::error::DataFusionError;

use datafusion::arrow::datatypes::DataType;
use datafusion_expr::{lit, BuiltinScalarFunction, Expr};

use datafusion::scalar::ScalarValue;

pub use datafusion_expr::LogicalPlan;

use datafusion::prelude::Column;

use std::sync::Arc;

/// An PyExpr that can be used on a DataFrame
#[pyclass(name = "Expression", module = "datafusion", subclass)]
#[derive(Debug, Clone)]
pub struct PyExpr {
    pub input_plan: Option<Arc<LogicalPlan>>,
    pub expr: Expr,
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
    pub fn from(expr: Expr, input: Option<Arc<LogicalPlan>>) -> PyExpr {
        PyExpr {
            input_plan: input,
            expr: expr,
        }
    }

    fn _column_name(&self, plan: LogicalPlan) -> String {
        match &self.expr {
            Expr::Alias(expr, name) => {
                // Only certain LogicalPlan variants are valid in this nested Alias scenario so we
                // extract the valid ones and error on the invalid ones
                match expr.as_ref() {
                    Expr::Column(col) => {
                        // First we must iterate the current node before getting its input
                        match plan {
                            LogicalPlan::Projection(proj) => {
                                match proj.input.as_ref() {
                                    LogicalPlan::Aggregate(agg) => {
                                        let mut exprs = agg.group_expr.clone();
                                        exprs.extend_from_slice(&agg.aggr_expr);
                                        let col_index: usize =
                                            proj.input.schema().index_of_column(col).unwrap();
                                        // match &exprs[plan.get_index(col)] {
                                        match &exprs[col_index] {
                                            Expr::AggregateFunction { args, .. } => {
                                                match &args[0] {
                                                    Expr::Column(col) => {
                                                        println!(
                                                            "AGGREGATE COLUMN IS {}",
                                                            col.name
                                                        );
                                                        col.name.clone()
                                                    }
                                                    _ => name.clone(),
                                                }
                                            }
                                            _ => name.clone(),
                                        }
                                    }
                                    _ => {
                                        println!("Encountered a non-Aggregate type");

                                        name.clone()
                                    }
                                }
                            }
                            _ => name.clone(),
                        }
                    }
                    _ => {
                        println!("Encountered a non Expr::Column instance");
                        name.clone()
                    }
                }
            }
            Expr::Column(column) => column.name.clone(),
            Expr::ScalarVariable(..) => unimplemented!("ScalarVariable!!!"),
            Expr::Literal(..) => unimplemented!("Literal!!!"),
            Expr::BinaryExpr {
                left: _,
                op: _,
                right: _,
            } => {
                // /// TODO: Examine this more deeply about whether name comes from the left or right
                // self.column_name(left)
                unimplemented!("BinaryExpr HERE!!!")
            }
            Expr::Not(..) => unimplemented!("Not!!!"),
            Expr::IsNotNull(..) => unimplemented!("IsNotNull!!!"),
            Expr::Negative(..) => unimplemented!("Negative!!!"),
            Expr::GetIndexedField { .. } => unimplemented!("GetIndexedField!!!"),
            Expr::IsNull(..) => unimplemented!("IsNull!!!"),
            Expr::Between { .. } => unimplemented!("Between!!!"),
            Expr::Case { .. } => unimplemented!("Case!!!"),
            Expr::Cast { .. } => unimplemented!("Cast!!!"),
            Expr::TryCast { .. } => unimplemented!("TryCast!!!"),
            Expr::Sort { .. } => unimplemented!("Sort!!!"),
            Expr::ScalarFunction { .. } => unimplemented!("ScalarFunction!!!"),
            Expr::AggregateFunction { .. } => unimplemented!("AggregateFunction!!!"),
            Expr::WindowFunction { .. } => unimplemented!("WindowFunction!!!"),
            Expr::AggregateUDF { .. } => unimplemented!("AggregateUDF!!!"),
            Expr::InList { .. } => unimplemented!("InList!!!"),
            Expr::Wildcard => unimplemented!("Wildcard!!!"),
            _ => panic!("Nothing found!!!"),
        }
    }
}

#[pymethods]
impl PyExpr {
    #[staticmethod]
    pub fn literal(value: PyScalarValue) -> PyExpr {
        PyExpr::from(lit(value.scalar_value), None)
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
        println!("&self: {:?}", &self);
        println!("&self.input_plan: {:?}", self.input_plan);
        let input: &Option<Arc<LogicalPlan>> = &self.input_plan;
        match input {
            Some(plan) => {
                let name: Result<String, DataFusionError> = self.expr.name(plan.schema());
                match name {
                    Ok(fq_name) => Ok(plan
                        .schema()
                        .index_of_column(&Column::from_qualified_name(&fq_name))
                        .unwrap()),
                    Err(e) => panic!("{:?}", e),
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
            Expr::Sort { .. } => panic!("Sort!!!"),
            Expr::ScalarFunction { .. } => "ScalarFunction",
            Expr::AggregateFunction { .. } => "AggregateFunction",
            Expr::WindowFunction { .. } => panic!("WindowFunction!!!"),
            Expr::AggregateUDF { .. } => panic!("AggregateUDF!!!"),
            Expr::InList { .. } => panic!("InList!!!"),
            Expr::Wildcard => panic!("Wildcard!!!"),
            _ => "OTHER",
        })
    }

    /// Determines the type of this Expr based on its variant
    #[pyo3(name = "getRexType")]
    pub fn rex_type(&self) -> RexType {
        match &self.expr {
            Expr::Alias(expr, name) => RexType::Reference,
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
            _ => RexType::Other,
        }
    }

    /// Python friendly shim code to get the name of a column referenced by an expression
    pub fn column_name(&self, mut plan: logical::PyLogicalPlan) -> String {
        self._column_name(plan.current_node())
    }

    /// Gets the operands for a BinaryExpr call
    #[pyo3(name = "getOperands")]
    pub fn get_operands(&self) -> PyResult<Vec<PyExpr>> {
        match &self.expr {
            Expr::BinaryExpr { left, op: _, right } => {
                let mut operands: Vec<PyExpr> = Vec::new();
                let left_desc: Expr = *left.clone();
                let py_left: PyExpr = PyExpr::from(left_desc, self.input_plan.clone());
                operands.push(py_left);
                let right_desc: Expr = *right.clone();
                let py_right: PyExpr = PyExpr::from(right_desc, self.input_plan.clone());
                operands.push(py_right);
                Ok(operands)
            }
            Expr::ScalarFunction { fun: _, args } => {
                let mut operands: Vec<PyExpr> = Vec::new();
                for arg in args {
                    let py_arg: PyExpr = PyExpr::from(arg.clone(), self.input_plan.clone());
                    operands.push(py_arg);
                }
                Ok(operands)
            }
            Expr::Cast { expr, data_type: _ } => {
                let mut operands: Vec<PyExpr> = Vec::new();
                let ex: Expr = *expr.clone();
                let py_ex: PyExpr = PyExpr::from(ex, self.input_plan.clone());
                operands.push(py_ex);
                Ok(operands)
            }
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "unknown Expr type encountered",
            )),
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
            Expr::Cast {
                expr: _,
                data_type: _,
            } => Ok(String::from("cast")),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Catch all triggered ....",
            )),
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
