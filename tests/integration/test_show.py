import pandas as pd
import pytest

from dask_sql import Context
from dask_sql.utils import ParsingException
from tests.utils import assert_eq


def test_schemas(c):
    expected_df = pd.DataFrame({"Schema": [c.schema_name, "information_schema"]})

    assert_eq(c.sql("SHOW SCHEMAS"), expected_df)
    assert_eq(c.sql(f"SHOW SCHEMAS FROM {c.catalog_name}"), expected_df)

    expected_df = pd.DataFrame({"Schema": ["information_schema"]})

    assert_eq(
        c.sql("SHOW SCHEMAS LIKE 'information_schema'"), expected_df, check_index=False
    )
    assert_eq(
        c.sql(f"SHOW SCHEMAS FROM {c.catalog_name} LIKE 'information_schema'"),
        expected_df,
        check_index=False,
    )


def test_tables(c):
    c.create_schema("test_schema")
    c.create_table("table", pd.DataFrame(), schema_name="test_schema")

    expected_df = pd.DataFrame({"Table": ["table"]})

    assert_eq(c.sql('SHOW TABLES FROM "test_schema"'), expected_df, check_index=False)
    assert_eq(
        c.sql(f'SHOW TABLES FROM "{c.catalog_name}"."test_schema"'),
        expected_df,
        check_index=False,
    )


def test_columns(c):
    result_df = c.sql(f'SHOW COLUMNS FROM "{c.schema_name}"."user_table_1"')
    expected_df = pd.DataFrame(
        {
            "Column": [
                "user_id",
                "b",
            ],
            "Type": ["bigint", "bigint"],
            "Extra": [""] * 2,
            "Comment": [""] * 2,
        }
    )

    assert_eq(result_df, expected_df)

    result_df = c.sql('SHOW COLUMNS FROM "user_table_1"')

    assert_eq(result_df, expected_df)


def test_wrong_input(c):
    with pytest.raises(KeyError):
        c.sql('SHOW COLUMNS FROM "wrong"."table"')
    with pytest.raises(ParsingException):
        c.sql('SHOW COLUMNS FROM "wrong"."table"."column"')
    with pytest.raises(KeyError):
        c.sql(f'SHOW COLUMNS FROM "{c.schema_name}"."table"')
    with pytest.raises(AttributeError):
        c.sql('SHOW TABLES FROM "wrong"')
    with pytest.raises(RuntimeError):
        c.sql(f'SHOW TABLES FROM "wrong"."{c.schema_name}"')
    with pytest.raises(RuntimeError):
        c.sql('SHOW SCHEMAS FROM "wrong"')


def test_show_tables(c):
    c = Context()

    df = pd.DataFrame({"id": [0, 1]})
    c.create_table("test", df)

    expected_df = pd.DataFrame({"Table": ["test"]})

    # no schema specified
    assert_eq(c.sql("show tables"), expected_df)

    # unqualified schema
    assert_eq(c.sql("show tables from root"), expected_df)

    # qualified schema
    assert_eq(c.sql("show tables from dask_sql.root"), expected_df)
