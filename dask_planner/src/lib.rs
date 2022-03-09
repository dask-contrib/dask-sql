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
    m.add_class::<sql::DaskSQLNode>()?;
    m.add_class::<sql::PyLogicalPlan>()?;
    m.add_class::<sql::DaskRelDataType>()?;
    m.add_class::<sql::DaskRelDataTypeField>()?;

    let sql_functions = PyModule::new(py, "sql_functions")?;
    sql::init_module(sql_functions)?;
    m.add_submodule(sql_functions)?;

    // // Register the functions as a submodule
    let funcs = PyModule::new(py, "functions")?;
    functions::init_module(funcs)?;
    m.add_submodule(funcs)?;

    Ok(())
}







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
//                         row_type: DaskRelDataType{
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
