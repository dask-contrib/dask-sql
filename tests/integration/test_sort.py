from tests.integration.fixtures import long_table
import pytest

from pandas.testing import assert_frame_equal

import pandas as pd
import dask.dataframe as dd


def test_sort(c, user_table_1):
    df = c.sql(
        """
    SELECT
        *
    FROM user_table_1
    ORDER BY b, user_id DESC
    """
    )
    df = df.compute().reset_index(drop=True)
    df_expected = user_table_1.sort_values(
        ["b", "user_id"], ascending=[True, False]
    ).reset_index(drop=True)

    assert_frame_equal(df, df_expected)


def test_sort_by_alias(c, user_table_1):
    df = c.sql(
        """
    SELECT
        b AS my_column
    FROM user_table_1
    ORDER BY my_column, user_id DESC
    """
    )
    df = df.compute().reset_index(drop=True).rename(columns={"my_column": "b"})
    df_expected = user_table_1.sort_values(
        ["b", "user_id"], ascending=[True, False]
    ).reset_index(drop=True)[["b"]]

    assert_frame_equal(df, df_expected)


def test_sort_with_nan(c):
    with pytest.raises(ValueError):
        c.sql(
            """
        SELECT
            *
        FROM user_table_nan
        ORDER BY c
        """
        )

    with pytest.raises(ValueError):
        c.sql(
            """
        SELECT
            *
        FROM user_table_inf
        ORDER BY c
        """
        )


def test_sort_strings(c):
    string_table = pd.DataFrame({"a": ["zzhsd", "öfjdf", "baba"]})
    c.create_table("string_table", string_table)

    df = c.sql(
        """
    SELECT
        *
    FROM string_table
    ORDER BY a
    """
    )
    df = df.compute().reset_index(drop=True)
    df_expected = string_table.sort_values(["a"], ascending=True).reset_index(drop=True)

    assert_frame_equal(df, df_expected)


def test_sort_not_allowed(c):
    # No DESC implemented for the first column
    with pytest.raises(NotImplementedError):
        c.sql("SELECT * FROM user_table_1 ORDER BY b DESC")

    # Wrong column
    with pytest.raises(Exception):
        c.sql("SELECT * FROM user_table_1 ORDER BY 42")


def test_limit(c, long_table):
    df = c.sql("SELECT * FROM long_table LIMIT 101")
    df = df.compute()

    assert_frame_equal(df, long_table.iloc[:101])

    df = c.sql("SELECT * FROM long_table LIMIT 100")
    df = df.compute()

    assert_frame_equal(df, long_table.iloc[:100])

    df = c.sql("SELECT * FROM long_table LIMIT 100 OFFSET 99")
    df = df.compute()

    assert_frame_equal(df, long_table.iloc[99 : 99 + 100])

    df = c.sql("SELECT * FROM long_table LIMIT 100 OFFSET 100")
    df = df.compute()

    assert_frame_equal(df, long_table.iloc[100 : 100 + 100])

    df = c.sql("SELECT * FROM long_table LIMIT 101 OFFSET 101")
    df = df.compute()

    assert_frame_equal(df, long_table.iloc[101 : 101 + 101])
