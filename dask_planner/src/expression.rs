
use pyo3::PyMappingProtocol;
use pyo3::{basic::CompareOp, prelude::*, PyNumberProtocol, PyObjectProtocol};
use std::convert::{From, Into};

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_plan::{col, lit, Expr};

use datafusion::scalar::ScalarValue;

/// An PyExpr that can be used on a DataFrame
#[pyclass(name = "Expression", module = "datafusion", subclass)]
#[derive(Debug, Clone)]
pub struct PyExpr {
    pub expr: Expr,
}

impl From<PyExpr> for Expr {
    fn from(expr: PyExpr) -> Expr {
        expr.expr
    }
}

impl From<Expr> for PyExpr {
    fn from(expr: Expr) -> PyExpr {
        PyExpr { expr }
    }
}

#[pyproto]
impl PyNumberProtocol for PyExpr {
    fn __add__(lhs: PyExpr, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok((lhs.expr + rhs.expr).into())
    }

    fn __sub__(lhs: PyExpr, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok((lhs.expr - rhs.expr).into())
    }

    fn __truediv__(lhs: PyExpr, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok((lhs.expr / rhs.expr).into())
    }

    fn __mul__(lhs: PyExpr, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok((lhs.expr * rhs.expr).into())
    }

    fn __mod__(lhs: PyExpr, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok(lhs.expr.modulus(rhs.expr).into())
    }

    fn __and__(lhs: PyExpr, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok(lhs.expr.and(rhs.expr).into())
    }

    fn __or__(lhs: PyExpr, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok(lhs.expr.or(rhs.expr).into())
    }

    fn __invert__(&self) -> PyResult<PyExpr> {
        Ok(self.expr.clone().not().into())
    }
}

#[pyproto]
impl PyObjectProtocol for PyExpr {
    fn __richcmp__(&self, other: PyExpr, op: CompareOp) -> PyExpr {
        let expr = match op {
            CompareOp::Lt => self.expr.clone().lt(other.expr),
            CompareOp::Le => self.expr.clone().lt_eq(other.expr),
            CompareOp::Eq => self.expr.clone().eq(other.expr),
            CompareOp::Ne => self.expr.clone().not_eq(other.expr),
            CompareOp::Gt => self.expr.clone().gt(other.expr),
            CompareOp::Ge => self.expr.clone().gt_eq(other.expr),
        };
        expr.into()
    }

    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{}", self.expr))
    }
}

#[pymethods]
impl PyExpr {
    #[staticmethod]
    pub fn literal(value: ScalarValue) -> PyExpr {
        lit(value).into()
    }


    /// Examine the current/"self" PyExpr and return its "type"
    /// In this context a "type" is what Dask-SQL Python
    /// RexConverter plugin instance should be invoked to handle
    /// the Rex conversion
    pub fn get_expr_type(&self) -> String {
        match &self.expr {
            Expr::Alias(..) => String::from("Alias"),
            Expr::Column(..) => String::from("Column"),
            Expr::ScalarVariable(..) => panic!("ScalarVariable!!!"),
            Expr::Literal(..) => String::from("Literal"),
            Expr::BinaryExpr {..} => String::from("BinaryExpr"),
            Expr::Not(..) => panic!("Not!!!"),
            Expr::IsNotNull(..) => panic!("IsNotNull!!!"),
            Expr::Negative(..) => panic!("Negative!!!"),
            Expr::GetIndexedField{..} => panic!("GetIndexedField!!!"),
            Expr::IsNull(..) => panic!("IsNull!!!"),
            Expr::Between{..} => panic!("Between!!!"),
            Expr::Case{..} => panic!("Case!!!"),
            Expr::Cast{..} => panic!("Cast!!!"),
            Expr::TryCast{..} => panic!("TryCast!!!"),
            Expr::Sort{..} => panic!("Sort!!!"),
            Expr::ScalarFunction{..} => panic!("ScalarFunction!!!"),
            Expr::AggregateFunction{..} => panic!("AggregateFunction!!!"),
            Expr::WindowFunction{..} => panic!("WindowFunction!!!"),
            Expr::AggregateUDF{..} => panic!("AggregateUDF!!!"),
            Expr::InList{..} => panic!("InList!!!"),
            Expr::Wildcard => panic!("Wildcard!!!"),
            _ => String::from("OTHER")
        }
    }


    pub fn column_name(&self) -> String {
        match &self.expr {
            Expr::Alias(exprs, name) => {
                println!("Expressions: {:?}, Expression Name: {:?}", exprs, name);
                panic!("Alias")
            },
            Expr::Column(column) => { column.name.clone() },
            Expr::ScalarVariable(..) => panic!("ScalarVariable!!!"),
            Expr::Literal(..) => panic!("Literal!!!"),
            Expr::BinaryExpr {..} => panic!("BinaryExpr"),
            Expr::Not(..) => panic!("Not!!!"),
            Expr::IsNotNull(..) => panic!("IsNotNull!!!"),
            Expr::Negative(..) => panic!("Negative!!!"),
            Expr::GetIndexedField{..} => panic!("GetIndexedField!!!"),
            Expr::IsNull(..) => panic!("IsNull!!!"),
            Expr::Between{..} => panic!("Between!!!"),
            Expr::Case{..} => panic!("Case!!!"),
            Expr::Cast{..} => panic!("Cast!!!"),
            Expr::TryCast{..} => panic!("TryCast!!!"),
            Expr::Sort{..} => panic!("Sort!!!"),
            Expr::ScalarFunction{..} => panic!("ScalarFunction!!!"),
            Expr::AggregateFunction{..} => panic!("AggregateFunction!!!"),
            Expr::WindowFunction{..} => panic!("WindowFunction!!!"),
            Expr::AggregateUDF{..} => panic!("AggregateUDF!!!"),
            Expr::InList{..} => panic!("InList!!!"),
            Expr::Wildcard => panic!("Wildcard!!!"),
            _ => panic!("Nothing found!!!")
        }
    }


    /// Gets the operands for a BinaryExpr call
    pub fn getOperands(&self) -> PyResult<Vec<PyExpr>> {
        match &self.expr {
            Expr::BinaryExpr {left, op, right} => {
                let mut operands: Vec<PyExpr> = Vec::new();
                let left_desc: Expr = *left.clone();
                operands.push(left_desc.into());
                let right_desc: Expr = *right.clone();
                operands.push(right_desc.into());
                Ok(operands)
            },
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>("current_node is not of type Projection"))
        }
    }

    pub fn getOperatorName(&self) -> PyResult<String> {
        match &self.expr {
            Expr::BinaryExpr { left, op, right } => {
                Ok(format!("{}", op))
            },
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>("Catch all triggered ...."))
        }
    }


    /// Gets the ScalarValue represented by the Expression
    pub fn getType(&self) -> PyResult<String> {
        match &self.expr {
            Expr::ScalarVariable(..) => panic!("ScalarVariable!!!"),
            Expr::Literal(scalarValue) => {
                let value = match scalarValue {
                    ScalarValue::Boolean(value) => {
                        Ok(String::from("Boolean"))
                    },
                    ScalarValue::Float32(value) => {
                        Ok(String::from("Float32"))
                    },
                    ScalarValue::Float64(value) => {
                        Ok(String::from("Float64"))
                    },
                    ScalarValue::Decimal128(value, ..) => {
                        Ok(String::from("Decimal128"))
                    },
                    ScalarValue::Int8(value) => {
                        Ok(String::from("Int8"))
                    },
                    ScalarValue::Int16(value) => {
                        Ok(String::from("Int16"))
                    },
                    ScalarValue::Int32(value) => {
                        Ok(String::from("Int32"))
                    },
                    ScalarValue::Int64(value) => {
                        Ok(String::from("Int64"))
                    },
                    ScalarValue::UInt8(value) => {
                        Ok(String::from("UInt8"))
                    },
                    ScalarValue::UInt16(value) => {
                        Ok(String::from("UInt16"))
                    },
                    ScalarValue::UInt32(value) => {
                        Ok(String::from("UInt32"))
                    },
                    ScalarValue::UInt64(value) => {
                        Ok(String::from("UInt64"))
                    },
                    ScalarValue::Utf8(value) => {
                        Ok(String::from("Utf8"))
                    },
                    ScalarValue::LargeUtf8(value) => {
                        Ok(String::from("LargeUtf8"))
                    },
                    ScalarValue::Binary(value) => {
                        Ok(String::from("Binary"))
                    },
                    ScalarValue::LargeBinary(value) => {
                        Ok(String::from("LargeBinary"))
                    },
                    ScalarValue::Date32(value) => {
                        Ok(String::from("Date32"))
                    },
                    ScalarValue::Date64(value) => {
                        Ok(String::from("Date64"))
                    },
                    _ => {
                        panic!("CatchAll")
                    }
                };
                value
            },
            _ => panic!("OTHER")
        }
    }


    #[staticmethod]
    pub fn column(value: &str) -> PyExpr {
        col(value).into()
    }

    /// assign a name to the PyExpr
    pub fn alias(&self, name: &str) -> PyExpr {
        self.expr.clone().alias(name).into()
    }

    /// Create a sort PyExpr from an existing PyExpr.
    #[args(ascending = true, nulls_first = true)]
    pub fn sort(&self, ascending: bool, nulls_first: bool) -> PyExpr {
        self.expr.clone().sort(ascending, nulls_first).into()
    }

    pub fn is_null(&self) -> PyExpr {
        self.expr.clone().is_null().into()
    }

    pub fn cast(&self, to: DataType) -> PyExpr {
        // self.expr.cast_to() requires DFSchema to validate that the cast
        // is supported, omit that for now
        let expr = Expr::Cast {
            expr: Box::new(self.expr.clone()),
            data_type: to,
        };
        expr.into()
    }

    /// TODO: I can't express how much I dislike explicity listing all of these methods out
    /// but PyO3 makes it necessary since its annotations cannot be used in trait impl blocks
    pub fn getFloat32Value(&mut self) -> f32 {
        match &self.expr {
            Expr::Literal(scalar_value) => {
                match scalar_value {
                    ScalarValue::Float32(iv) => {
                        iv.unwrap()
                    },
                    _ => {
                        panic!("getValue<T>() - Unexpected value")
                    }
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered")
        }
    }

    pub fn getFloat64Value(&mut self) -> f64 {
        match &self.expr {
            Expr::Literal(scalar_value) => {
                match scalar_value {
                    ScalarValue::Float64(iv) => {
                        iv.unwrap()
                    },
                    _ => {
                        panic!("getValue<T>() - Unexpected value")
                    }
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered")
        }
    }

    pub fn getInt8Value(&mut self) -> i8 {
        match &self.expr {
            Expr::Literal(scalar_value) => {
                match scalar_value {
                    ScalarValue::Int8(iv) => {
                        iv.unwrap()
                    },
                    _ => {
                        panic!("getValue<T>() - Unexpected value")
                    }
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered")
        }
    }

    pub fn getInt16Value(&mut self) -> i16 {
        match &self.expr {
            Expr::Literal(scalar_value) => {
                match scalar_value {
                    ScalarValue::Int16(iv) => {
                        iv.unwrap()
                    },
                    _ => {
                        panic!("getValue<T>() - Unexpected value")
                    }
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered")
        }
    }

    pub fn getInt32Value(&mut self) -> i32 {
        match &self.expr {
            Expr::Literal(scalar_value) => {
                match scalar_value {
                    ScalarValue::Int32(iv) => {
                        iv.unwrap()
                    },
                    _ => {
                        panic!("getValue<T>() - Unexpected value")
                    }
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered")
        }
    }

    pub fn getInt64Value(&mut self) -> i64 {
        match &self.expr {
            Expr::Literal(scalar_value) => {
                match scalar_value {
                    ScalarValue::Int64(iv) => {
                        iv.unwrap()
                    },
                    _ => {
                        panic!("getValue<T>() - Unexpected value")
                    }
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered")
        }
    }


    pub fn getUInt8Value(&mut self) -> u8 {
        match &self.expr {
            Expr::Literal(scalar_value) => {
                match scalar_value {
                    ScalarValue::UInt8(iv) => {
                        iv.unwrap()
                    },
                    _ => {
                        panic!("getValue<T>() - Unexpected value")
                    }
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered")
        }
    }

    pub fn getUInt16Value(&mut self) -> u16 {
        match &self.expr {
            Expr::Literal(scalar_value) => {
                match scalar_value {
                    ScalarValue::UInt16(iv) => {
                        iv.unwrap()
                    },
                    _ => {
                        panic!("getValue<T>() - Unexpected value")
                    }
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered")
        }
    }

    pub fn getUInt32Value(&mut self) -> u32 {
        match &self.expr {
            Expr::Literal(scalar_value) => {
                match scalar_value {
                    ScalarValue::UInt32(iv) => {
                        iv.unwrap()
                    },
                    _ => {
                        panic!("getValue<T>() - Unexpected value")
                    }
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered")
        }
    }

    pub fn getUInt64Value(&mut self) -> u64 {
        match &self.expr {
            Expr::Literal(scalar_value) => {
                match scalar_value {
                    ScalarValue::UInt64(iv) => {
                        iv.unwrap()
                    },
                    _ => {
                        panic!("getValue<T>() - Unexpected value")
                    }
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered")
        }
    }

    pub fn getBoolValue(&mut self) -> bool {
        match &self.expr {
            Expr::Literal(scalar_value) => {
                match scalar_value {
                    ScalarValue::Boolean(iv) => {
                        iv.unwrap()
                    },
                    _ => {
                        panic!("getValue<T>() - Unexpected value")
                    }
                }
            },
            _ => panic!("getValue<T>() - Non literal value encountered")
        }
    }


// get_typed_value!(i8, Int8);
// get_typed_value!(i16, Int16);
// get_typed_value!(i32, Int32);
// get_typed_value!(i64, Int64);
// get_typed_value!(bool, Boolean);
// get_typed_value!(f32, Float32);
// get_typed_value!(f64, Float64);
}

// pub trait ObtainValue<T> {
//     fn getValue(&mut self) -> T;
// }


// /// Expansion macro to get all typed values from a Datafusion Expr
// macro_rules! get_typed_value {
//     ($t:ty, $func_name:ident) => {
//         impl ObtainValue<$t> for PyExpr {
//             #[inline]
//             fn getValue(&mut self) -> $t
//             {
//                 match &self.expr {
//                     Expr::Literal(scalar_value) => {
//                         match scalar_value {
//                             ScalarValue::$func_name(iv) => {
//                                 iv.unwrap()
//                             },
//                             _ => {
//                                 panic!("getValue<T>() - Unexpected value")
//                             }
//                         }
//                     },
//                     _ => panic!("getValue<T>() - Non literal value encountered")
//                 }
//             }
//         }
//     }
// }

// get_typed_value!(u8, UInt8);
// get_typed_value!(u16, UInt16);
// get_typed_value!(u32, UInt32);
// get_typed_value!(u64, UInt64);
// get_typed_value!(i8, Int8);
// get_typed_value!(i16, Int16);
// get_typed_value!(i32, Int32);
// get_typed_value!(i64, Int64);
// get_typed_value!(bool, Boolean);
// get_typed_value!(f32, Float32);
// get_typed_value!(f64, Float64);


// get_typed_value!(for usize u8 u16 u32 u64 isize i8 i16 i32 i64 bool f32 f64);
// get_typed_value!(usize, Integer);
// get_typed_value!(isize, );
// Decimal128(Option<i128>, usize, usize),
// Utf8(Option<String>),
// LargeUtf8(Option<String>),
// Binary(Option<Vec<u8, Global>>),
// LargeBinary(Option<Vec<u8, Global>>),
// List(Option<Box<Vec<ScalarValue, Global>, Global>>, Box<DataType, Global>),
// Date32(Option<i32>),
// Date64(Option<i64>),


#[pyproto]
impl PyMappingProtocol for PyExpr {
    fn __getitem__(&self, key: &str) -> PyResult<PyExpr> {
        Ok(Expr::GetIndexedField {
            expr: Box::new(self.expr.clone()),
            key: ScalarValue::Utf8(Some(key.to_string())),
        }
        .into())
    }
}
