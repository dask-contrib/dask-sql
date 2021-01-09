import pytest

import pandas as pd
from pandas.testing import assert_frame_equal


def test_schemas(c):
    df = c.sql("SHOW SCHEMAS")
    df = df.compute()

    expected_df = pd.DataFrame({"Schema": [c.schema_name, "information_schema"]})

    assert_frame_equal(df, expected_df)

    df = c.sql("SHOW SCHEMAS LIKE 'information_schema'")
    df = df.compute()

    expected_df = pd.DataFrame({"Schema": ["information_schema"]})

    assert_frame_equal(df.reset_index(drop=True), expected_df.reset_index(drop=True))


def test_tables(c):
    df = c.sql(f'SHOW TABLES FROM "{c.schema_name}"')
    df = df.compute()

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
        }
    )

    assert_frame_equal(
        df.sort_values("Table").reset_index(drop=True),
        expected_df.sort_values("Table").reset_index(drop=True),
    )


def test_columns(c):
    df = c.sql(f'SHOW COLUMNS FROM "{c.schema_name}"."user_table_1"')
    df = df.compute()

    expected_df = pd.DataFrame(
        {
            "Column": ["user_id", "b",],
            "Type": ["bigint", "bigint"],
            "Extra": [""] * 2,
            "Comment": [""] * 2,
        }
    )

    assert_frame_equal(df.sort_values("Column"), expected_df.sort_values("Column"))

    df = c.sql('SHOW COLUMNS FROM "user_table_1"')
    df = df.compute()
    assert_frame_equal(df.sort_values("Column"), expected_df.sort_values("Column"))


def test_wrong_input(c):
    with pytest.raises(AttributeError):
        c.sql('SHOW COLUMNS FROM "wrong"."table"')
    with pytest.raises(AttributeError):
        c.sql('SHOW COLUMNS FROM "wrong"."table"."column"')
    with pytest.raises(AttributeError):
        c.sql(f'SHOW COLUMNS FROM "{c.schema_name}"."table"')
    with pytest.raises(AttributeError):
        c.sql('SHOW TABLES FROM "wrong"')
