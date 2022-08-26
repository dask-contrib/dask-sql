use crate::sql::exceptions::py_type_err;
use crate::sql::logical;

use datafusion_expr::logical_plan::CreateCatalogSchema;

use pyo3::prelude::*;

#[pyclass(name = "CreateCatalogSchema", module = "dask_planner", subclass)]
pub struct PyCreateCatalogSchema {
    pub(crate) create_catalog_schema: CreateCatalogSchema,
}

#[pymethods]
impl PyCreateCatalogSchema {
    #[pyo3(name = "getSchemaName")]
    fn get_table_name(&self) -> PyResult<String> {
        Ok(self.create_catalog_schema.schema_name.clone())
    }

    #[pyo3(name = "getIfNotExists")]
    fn get_if_not_exists(&self) -> PyResult<bool> {
        Ok(self.create_catalog_schema.if_not_exists)
    }
}

impl TryFrom<logical::LogicalPlan> for PyCreateCatalogSchema {
    type Error = PyErr;

    fn try_from(logical_plan: logical::LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            logical::LogicalPlan::CreateCatalogSchema(create_catalog_schema) => {
                Ok(PyCreateCatalogSchema {
                    create_catalog_schema,
                })
            }
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
