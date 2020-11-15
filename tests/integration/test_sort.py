from tests.integration.fixtures import long_table
import pytest

from pandas.testing import assert_frame_equal

import pandas as pd
import dask.dataframe as dd


def test_sort(c, user_table_1, df):
    df_result = c.sql(
        """
    SELECT
        *
    FROM user_table_1
    ORDER BY b, user_id DESC
    """
    )
    df_result = df_result.compute().reset_index(drop=True)
    df_expected = user_table_1.sort_values(
        ["b", "user_id"], ascending=[True, False]
    ).reset_index(drop=True)

    assert_frame_equal(df_result, df_expected)

    df_result = c.sql(
        """
    SELECT
        *
    FROM df
    ORDER BY b DESC, a DESC
    """
    )
    df_result = df_result.compute()
    df_expected = df.sort_values(["b", "a"], ascending=[False, False])

    assert_frame_equal(
        df_result.reset_index(drop=True), df_expected.reset_index(drop=True)
    )

    df_result = c.sql(
        """
    SELECT
        *
    FROM df
    ORDER BY a DESC, b
    """
    )
    df_result = df_result.compute()
    df_expected = df.sort_values(["a", "b"], ascending=[False, True])

    assert_frame_equal(
        df_result.reset_index(drop=True), df_expected.reset_index(drop=True)
    )

    df_result = c.sql(
        """
    SELECT
        *
    FROM df
    ORDER BY b, a
    """
    )
    df_result = df_result.compute()
    df_expected = df.sort_values(["b", "a"], ascending=[True, True])

    assert_frame_equal(
        df_result.reset_index(drop=True), df_expected.reset_index(drop=True)
    )


def test_sort_by_alias(c, user_table_1):
    df_result = c.sql(
        """
    SELECT
        b AS my_column
    FROM user_table_1
    ORDER BY my_column, user_id DESC
    """
    )
    df_result = (
        df_result.compute().reset_index(drop=True).rename(columns={"my_column": "b"})
    )
    df_expected = user_table_1.sort_values(
        ["b", "user_id"], ascending=[True, False]
    ).reset_index(drop=True)[["b"]]

    assert_frame_equal(df_result, df_expected)


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

    df_result = c.sql(
        """
    SELECT
        *
    FROM string_table
    ORDER BY a
    """
    )
    df_result = df_result.compute().reset_index(drop=True)
    df_expected = string_table.sort_values(["a"], ascending=True).reset_index(drop=True)

    assert_frame_equal(df_result, df_expected)


def test_sort_not_allowed(c):
    # Wrong column
    with pytest.raises(Exception):
        c.sql("SELECT * FROM user_table_1 ORDER BY 42")


def test_limit(c, long_table):
    df_result = c.sql("SELECT * FROM long_table LIMIT 101")
    df_result = df_result.compute()

    assert_frame_equal(df_result, long_table.iloc[:101])

    df_result = c.sql("SELECT * FROM long_table LIMIT 100")
    df_result = df_result.compute()

    assert_frame_equal(df_result, long_table.iloc[:100])

    df_result = c.sql("SELECT * FROM long_table LIMIT 100 OFFSET 99")
    df_result = df_result.compute()

    assert_frame_equal(df_result, long_table.iloc[99 : 99 + 100])

    df_result = c.sql("SELECT * FROM long_table LIMIT 100 OFFSET 100")
    df_result = df_result.compute()

    assert_frame_equal(df_result, long_table.iloc[100 : 100 + 100])

    df_result = c.sql("SELECT * FROM long_table LIMIT 101 OFFSET 101")
    df_result = df_result.compute()

    assert_frame_equal(df_result, long_table.iloc[101 : 101 + 101])
