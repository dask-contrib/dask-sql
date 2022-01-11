import pandas as pd
import pytest
from dask.dataframe.utils import assert_eq

try:
    import cudf
except ImportError:
    cudf = None


def test_schemas(c):
    result_df = c.sql("SHOW SCHEMAS")
    expected_df = pd.DataFrame({"Schema": [c.schema_name, "information_schema"]})

    assert_eq(result_df, expected_df)

    result_df = c.sql("SHOW SCHEMAS LIKE 'information_schema'")
    expected_df = pd.DataFrame({"Schema": ["information_schema"]})

    assert_eq(result_df, expected_df, check_index=False)


def test_tables(c):
    result_df = c.sql(f'SHOW TABLES FROM "{c.schema_name}"')
    expected_df = pd.DataFrame(
        {
            "Table": [
                "df",
                "df_simple",
                "user_table_1",
                "user_table_2",
                "long_table",
                "user_table_inf",
                "user_table_nan",
                "string_table",
                "datetime_table",
            ]
            if cudf is None
            else [
                "df",
                "df_simple",
                "user_table_1",
                "user_table_2",
                "long_table",
                "user_table_inf",
                "user_table_nan",
                "string_table",
                "datetime_table",
                "gpu_user_table_1",
                "gpu_df",
                "gpu_long_table",
                "gpu_string_table",
            ]
        }
    )

    assert_eq(result_df, expected_df, check_index=False)


def test_columns(c):
    result_df = c.sql(f'SHOW COLUMNS FROM "{c.schema_name}"."user_table_1"')
    expected_df = pd.DataFrame(
        {
            "Column": ["user_id", "b",],
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
    with pytest.raises(AttributeError):
        c.sql('SHOW COLUMNS FROM "wrong"."table"."column"')
    with pytest.raises(KeyError):
        c.sql(f'SHOW COLUMNS FROM "{c.schema_name}"."table"')
    with pytest.raises(AttributeError):
        c.sql('SHOW TABLES FROM "wrong"')
