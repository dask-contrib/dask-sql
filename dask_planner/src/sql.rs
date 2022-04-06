pub mod types;
pub mod table;
pub mod logical;
pub mod column;
pub mod statement;
pub mod schema;
pub mod function;

use datafusion::logical_plan::plan::{
    LogicalPlan,
    TableScan,
    Projection,
};

use std::collections::HashMap;

use pyo3::prelude::*;

use datafusion::sql::parser::{DFParser};

use datafusion::arrow::datatypes::{Field, Schema};

use datafusion::catalog::TableReference;
use datafusion::sql::planner::{SqlToRel};

use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::physical_plan::udaf::AggregateUDF;

use std::sync::Arc;

use crate::expression::PyExpr;


/// DaskSQLContext is main interface used for interacting with Datafusion to
/// parse SQL queries, build logical plans, and optimize logical plans.
///
/// The following example demonstrates how to generate an optimized LogicalPlan
/// from SQL using DaskSQLContext.
///
/// ```
/// use datafusion::prelude::*;
///
/// # use datafusion::error::Result;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let mut ctx = DaskSQLContext::new();
/// let parsed_sql = ctx.parse_sql("SELECT COUNT(*) FROM test_table");
/// let nonOptimizedRelAlgebra = ctx.logical_relational_algebra(parsed_sql);
/// let optmizedRelAlg = ctx.optimizeRelationalAlgebra(nonOptimizedRelAlgebra);
/// # Ok(())
/// # }
/// ```
#[pyclass(name = "DaskSQLContext", module = "dask_planner", subclass)]
#[derive(Clone)]
pub struct DaskSQLContext {
    default_schema_name: String,
    pub schemas: HashMap<String, schema::DaskSchema>,
}

impl datafusion::sql::planner::ContextProvider for DaskSQLContext {
    fn get_table_provider(
        &self,
        name: TableReference,
    ) -> Option<Arc<dyn table::TableProvider>> {
        match self.schemas.get(&String::from(&self.default_schema_name)) {
            Some(schema) => {
                let mut resp = None;
                let mut table_name: String = "".to_string();
                for (_table_name, table) in &schema.tables {
                    if table.name.eq(&name.table()) {
                        // Build the Schema here
                        let mut fields: Vec<Field> = Vec::new();

                        // Iterate through the DaskTable instance and create a Schema instance
                        for (column_name, column_type) in &table.columns {
                            fields.push(Field::new(column_name, column_type.sql_type.clone(), false));
                        }

                        resp = Some(Schema::new(fields));
                        table_name = _table_name.clone();
                    }
                }
                Some(Arc::new(table::DaskTableProvider::new(
                    Arc::new(
                        resp.unwrap(),
                    ),
                    table_name,
                )))
            },
            None => panic!("Schema with name {} not found", "table_name"),
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        println!("RUST: get_function_meta");
        let _f: datafusion::physical_plan::functions::ScalarFunctionImplementation =
            Arc::new(|_| Err(datafusion::error::DataFusionError::NotImplemented("".to_string())));
        match name {
            _ => None,
        }
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        println!("RUST: get_aggregate_meta NEED TO MAKE SURE THIS IS IMPLEMENTED LATER!!!!");
        // unimplemented!()
        None
    }
}


#[pymethods]
impl DaskSQLContext {
    #[new]
    pub fn new(default_schema_name: String) -> Self {
        Self {
            default_schema_name: default_schema_name,
            schemas: HashMap::new(),
        }
    }

    pub fn register_schema(&mut self, schema_name:String, schema: schema::DaskSchema) {
        self.schemas.insert(schema_name, schema);
    }

    pub fn register_table(&mut self, schema_name:String, table: table::DaskTable) {
        match self.schemas.get_mut(&schema_name) {
            Some(schema) => schema.add_table(table),
            None => println!("Schema: {} not found in DaskSQLContext", schema_name),
        }
    }

    /// Parses a SQL string into an AST presented as a Vec of Statements
    pub fn parse_sql(&self, sql: &str) -> Vec<statement::PyStatement> {
        match DFParser::parse_sql(sql) {
            Ok(k) => {
                let mut statements = Vec::new();
                for statement in k {
                    println!("Statement: {:?}\n", statement);
                    statements.push(statement.into());
                }
                assert!(statements.len() == 1, "More than 1 expected statement was encounterd!");
                statements
            },
            Err(e) => panic!("{}", e.to_string()),
        }
    }

    /// Creates a non-optimized Relational Algebra LogicalPlan from an AST Statement
    pub fn logical_relational_algebra(&self, statement: statement::PyStatement) -> logical::PyLogicalPlan {
        let planner = SqlToRel::new(self);

        println!("Input Statement: {:?}", &statement.statement);

        match planner.statement_to_plan(&statement.statement) {
            Ok(k) => {
                println!("----Full Logical Plan----\n{:?}\n-------------------", k);
                logical::PyLogicalPlan {
                    original_plan: k,
                    current_node: None,
                }
            },
            Err(e) => panic!("{}", e.to_string()),
        }
    }
}
