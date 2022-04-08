import logging

import pandas as pd

from dask_sql.context import Context

logger = logging.getLogger(__name__)


def create_meta_data(c: Context):
    """
    Creates the schema, table and column data for prestodb JDBC driver so that data can be viewed
    in a database tool like DBeaver. It doesn't create a catalog entry although JDBC expects one
    as dask-sql doesn't support catalogs. For both catalogs and procedures empty placeholder
    tables are created.

    The meta-data appears in a separate schema called system_jdbc largely because the JDBC driver
    tries to access system.jdbc and it sufficiently so shouldn't clash with other schemas.

    A function is required in the /v1/statement to change system.jdbc to system_jdbc and ignore
    order by statements from the driver (as adjust_for_presto_sql above)

    :param c: Context containing created tables
    :return:
    """

    if c is None:
        logger.warn("Context None: jdbc meta data not created")
        return
    catalog = ""
    system_schema = "system_jdbc"
    c.create_schema(system_schema)

    # TODO: add support for catalogs in presto interface
    # see https://github.com/dask-contrib/dask-sql/pull/351
    # if catalog and len(catalog.strip()) > 0:
    #     catalogs = pd.DataFrame().append(create_catalog_row(catalog), ignore_index=True)
    #     c.create_table("catalogs", catalogs, schema_name=system_schema)

    schemas = pd.DataFrame().append(create_schema_row(), ignore_index=True)
    c.create_table("schemas", schemas, schema_name=system_schema)
    schema_rows = []

    tables = pd.DataFrame().append(create_table_row(), ignore_index=True)
    c.create_table("tables", tables, schema_name=system_schema)
    table_rows = []

    columns = pd.DataFrame().append(create_column_row(), ignore_index=True)
    c.create_table("columns", columns, schema_name=system_schema)
    column_rows = []

    for schema_name, schema in c.schema.items():
        schema_rows.append(create_schema_row(catalog, schema_name))
        for table_name, dc in schema.tables.items():
            df = dc.df
            logger.info(f"schema ${schema_name}, table {table_name}, {df}")
            table_rows.append(create_table_row(catalog, schema_name, table_name))
            pos: int = 0
            for column in df.columns:
                pos = pos + 1
                logger.debug(f"column {column}")
                dtype = "VARCHAR"
                if df[column].dtype == "int64" or df[column].dtype == "int":
                    dtype = "INTEGER"
                elif df[column].dtype == "float64" or df[column].dtype == "float":
                    dtype = "FLOAT"
                elif (
                    df[column].dtype == "datetime"
                    or df[column].dtype == "datetime64[ns]"
                ):
                    dtype = "TIMESTAMP"
                column_rows.append(
                    create_column_row(
                        catalog,
                        schema_name,
                        table_name,
                        dtype,
                        df[column].name,
                        str(pos),
                    )
                )

    schemas = pd.DataFrame(schema_rows)
    c.create_table("schemas", schemas, schema_name=system_schema)
    tables = pd.DataFrame(table_rows)
    c.create_table("tables", tables, schema_name=system_schema)
    columns = pd.DataFrame(column_rows)
    c.create_table("columns", columns, schema_name=system_schema)

    logger.info(f"jdbc meta data ready for {len(table_rows)} tables")


def create_catalog_row(catalog: str = ""):
    return {"TABLE_CAT": catalog}


def create_schema_row(catalog: str = "", schema: str = ""):
    return {"TABLE_CATALOG": catalog, "TABLE_SCHEM": schema}


def create_table_row(catalog: str = "", schema: str = "", table: str = ""):
    return {
        "TABLE_CAT": catalog,
        "TABLE_SCHEM": schema,
        "TABLE_NAME": table,
        "TABLE_TYPE": "",
        "REMARKS": "",
        "TYPE_CAT": "",
        "TYPE_SCHEM": "",
        "TYPE_NAME": "",
        "SELF_REFERENCING_COL_NAME": "",
        "REF_GENERATION": "",
    }


def create_column_row(
    catalog: str = "",
    schema: str = "",
    table: str = "",
    dtype: str = "",
    column: str = "",
    pos: str = "",
):
    return {
        "TABLE_CAT": catalog,
        "TABLE_SCHEM": schema,
        "TABLE_NAME": table,
        "COLUMN_NAME": column,
        "DATA_TYPE": dtype,
        "TYPE_NAME": dtype,
        "COLUMN_SIZE": "",
        "BUFFER_LENGTH": "",
        "DECIMAL_DIGITS": "",
        "NUM_PREC_RADIX": "",
        "NULLABLE": "",
        "REMARKS": "",
        "COLUMN_DEF": "",
        "SQL_DATA_TYPE": dtype,
        "SQL_DATETIME_SUB": "",
        "CHAR_OCTET_LENGTH": "",
        "ORDINAL_POSITION": pos,
        "IS_NULLABLE": "",
        "SCOPE_CATALOG": "",
        "SCOPE_SCHEMA": "",
        "SCOPE_TABLE": "",
        "SOURCE_DATA_TYPE": "",
        "IS_AUTOINCREMENT": "",
        "IS_GENERATEDCOLUMN": "",
    }
