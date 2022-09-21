import pandas as pd
import pytest

from dask_sql import Context
from dask_sql.utils import ParsingException
from tests.utils import assert_eq


def test_schemas(c):
    result_df = c.sql("SHOW SCHEMAS")
    expected_df = pd.DataFrame({"Schema": [c.schema_name, "information_schema"]})

    assert_eq(result_df, expected_df)

    result_df = c.sql("SHOW SCHEMAS LIKE 'information_schema'")
    expected_df = pd.DataFrame({"Schema": ["information_schema"]})

    assert_eq(result_df, expected_df, check_index=False)


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_tables(gpu):
    c = Context()
    c.create_table("table", pd.DataFrame(), gpu=gpu)

    result_df = c.sql(f'SHOW TABLES FROM "{c.schema_name}"')
    expected_df = pd.DataFrame({"Table": ["table"]})

    assert_eq(result_df, expected_df, check_index=False)


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


def test_show_tables_no_schema(c):
    c = Context()

    df = pd.DataFrame({"id": [0, 1]})
    c.create_table("test", df)

    actual_df = c.sql("show tables").compute()
    expected_df = pd.DataFrame({"Table": ["test"]})
    assert_eq(actual_df, expected_df)
