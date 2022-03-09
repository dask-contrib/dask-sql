use mimalloc::MiMalloc;
use pyo3::prelude::*;

mod catalog;
mod errors;
mod expression;
mod functions;
mod sql;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Low-level DataFusion internal package.
///
/// The higher-level public API is defined in pure python files under the
/// dask_planner directory.
#[pymodule]
fn rust(py: Python, m: &PyModule) -> PyResult<()> {
    // Register the python classes
    m.add_class::<catalog::PyCatalog>()?;
    m.add_class::<catalog::PyDatabase>()?;
    m.add_class::<catalog::PyTable>()?;
    m.add_class::<expression::PyExpr>()?;

    // SQL specific classes
    m.add_class::<sql::PyStatement>()?;
    m.add_class::<sql::PyQuery>()?;
    m.add_class::<sql::DaskSchema>()?;
    m.add_class::<sql::DaskTable>()?;
    m.add_class::<sql::DaskFunction>()?;
    m.add_class::<sql::DaskStatistics>()?;

    let sql_functions = PyModule::new(py, "sql_functions")?;
    sql::init_module(sql_functions)?;
    m.add_submodule(sql_functions)?;

    // m.add_class::<DaskLogicalPlan>()?;
    // m.add_class::<DaskTable>()?;
    // m.add_class::<DaskRelRowType>()?;

    // // Register the functions as a submodule
    let funcs = PyModule::new(py, "functions")?;
    functions::init_module(funcs)?;
    m.add_submodule(funcs)?;



    // let submodule = PyModule::new(py, "submodule")?;
    // submodule.add("super_useful_constant", "important")?;

    // module.add_submodule(submodule)?;

    Ok(())
}














// use arrow::datatypes::{Schema, Field, DataType};

// use datafusion::catalog::TableReference;
// use datafusion::datasource::TableProvider;
// use datafusion::logical_plan::plan::LogicalPlan;
// use datafusion::sql::parser::{DFParser, Statement};
// use datafusion::sql::planner::{SqlToRel};

// use std::sync::Arc;

// use pyo3::prelude::*;
// use pyo3::exceptions;


// struct DaskSQLContextProvider {}

// impl datafusion::sql::planner::ContextProvider for DaskSQLContextProvider {
//     fn get_table_provider(
//         &self,
//         name: TableReference,
//     ) -> Option<Arc<dyn TableProvider>> {
//         let schema = match name.table() {
//             "person" => Some(Schema::new(vec![
//                 Field::new("first_name", DataType::Utf8, false),
//                 Field::new("last_name", DataType::Utf8, false),
//             ])),
//             _ => None,
//         };
//         schema.map(|s| -> Arc<dyn TableProvider> {
//             Arc::new(datafusion::datasource::empty::EmptyTable::new(Arc::new(s)))
//         })
//     }

//     fn get_function_meta(&self, name: &str) -> Option<Arc<datafusion::physical_plan::udf::ScalarUDF>> {
//         let _f: datafusion::physical_plan::functions::ScalarFunctionImplementation =
//             Arc::new(|_| Err(datafusion::error::DataFusionError::NotImplemented("".to_string())));
//         match name {
//             _ => None,
//         }
//     }

//     fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<datafusion::physical_plan::udaf::AggregateUDF>> {
//         unimplemented!()
//     }
// }

// #[pyclass]
// #[derive(Debug, Clone)]
// struct DaskRelRowType {
//     #[pyo3(get, set)]
//     field_names: Vec<String>,
// }

// #[pymethods]
// impl DaskRelRowType {
//     pub fn getFieldNames(&self) -> PyResult<Vec<String>> {
//         Ok(self.field_names.clone())
//     }
// }


// #[pyclass]
// #[derive(Debug, Clone)]
// struct DaskTable {
//     #[pyo3(get, set)]
//     schema_name: String,

//     #[pyo3(get, set)]
//     table_name: String,

//     #[pyo3(get, set)]
//     row_type: DaskRelRowType,
// }

// #[pymethods]
// impl DaskTable {
//     pub fn something(&self) -> PyResult<String> {
//         Ok(format!("something"))
//     }

//     pub fn getQualifiedName(&self) -> PyResult<Vec<String>> {
//         let mut qualified_name = Vec::new();
//         qualified_name.push(self.schema_name.clone());
//         qualified_name.push(self.table_name.clone());
//         Ok(qualified_name)
//     }

//     pub fn getRowType(&self) -> PyResult<DaskRelRowType> {
//         Ok(self.row_type.clone())
//     }
// }


// #[pyclass]
// #[derive(Debug, Clone)]
// struct DaskLogicalPlan {
//     original_sql: String,
//     table: DaskTable,
//     parsed_statements: Vec<Statement>,
//     logical_plan: LogicalPlan,
// }

// #[pymethods]
// impl DaskLogicalPlan {
//     pub fn getInputs(&self) -> PyResult<String> {
//         Ok(format!("RandomString ....."))
//     }

//     pub fn getTableName(&self) -> PyResult<String> {
//         Ok(self.table.table_name.clone())
//     }

//     pub fn getTable(&self) -> PyResult<DaskTable> {
//         Ok(self.table.clone())
//     }

//     pub fn getProjections(&self) -> PyResult<Vec<String>> {
//         let mut projected_cols = Vec::new();
//         for statement in &self.parsed_statements {
//             match statement {
//                 Statement::Statement(sql_statement) => {
//                     match &**sql_statement {
//                         sqlparser::ast::Statement::Query(query) => {
//                             match &query.body {
//                                 sqlparser::ast::SetExpr::Select(select) => {
//                                     for si in &select.projection {
//                                         match si {
//                                             sqlparser::ast::SelectItem::UnnamedExpr(expr) =>  {
//                                                 match expr {
//                                                     sqlparser::ast::Expr::Identifier(ident) => {
//                                                         projected_cols.push(String::from(&ident.value))
//                                                     },
//                                                     _ => println!("Doesn't matter"),
//                                                 }
//                                             },
//                                             _ => println!("Doesn't matter"),
//                                         }
//                                     }
//                                 },
//                                 _ => println!("nothing else matters"),
//                             }
//                         },
//                         _ => println!("Nothing matched"),
//                     }
//                 },
//                 Statement::CreateExternalTable(cet) => {println!("CREATE EXTERNAL TABLE INVOKED VIA QUERY!!!!!!");}
//             }
//         }
//         Ok(projected_cols)
//     }
// }

// #[pyfunction]
// fn get_sql_node(sql: String) -> PyResult<DaskLogicalPlan> {

//     let context_provider = &DaskSQLContextProvider {};
//     let planner = SqlToRel::new(context_provider);

//     let logical_plan = DFParser::parse_sql(&sql);

//     match logical_plan {
//         Ok(results) => {
//             match planner.statement_to_plan(&results[0]) {
//                 Ok(k) => Ok(DaskLogicalPlan{
//                     original_sql: sql,
//                     table: DaskTable{
//                         schema_name: String::from("root"),
//                         table_name: String::from("person"),
//                         row_type: DaskRelRowType{
//                             field_names: vec![String::from("first_name"), String::from("last_name")]
//                         },
//                     },
//                     logical_plan: k,
//                     parsed_statements: results
//                 }),
//                 Err(e) => Err(PyErr::new::<exceptions::PyTypeError, _>(e.to_string())),
//             }
//         },
//         Err(e) => {Err(PyErr::new::<exceptions::PyTypeError, _>(e.to_string()))},
//     }
// }


// /// A Python module implemented in Rust. The name of this function must match
// /// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
// /// import the module.
// #[pymodule]
// fn datafusion_planner(_py: Python, m: &PyModule) -> PyResult<()> {
//     m.add_class::<DaskLogicalPlan>()?;
//     m.add_class::<DaskTable>()?;
//     m.add_class::<DaskRelRowType>()?;
//     m.add_function(wrap_pyfunction!(get_sql_node, m)?)?;
//     Ok(())
// }
