import numpy as np
import pandas as pd
import pytest
from dask.dataframe.utils import assert_eq

from dask_sql.utils import ParsingException


def test_select(c, df):
    result_df = c.sql("SELECT * FROM df")

    assert_eq(result_df, df)


def test_select_alias(c, df):
    result_df = c.sql("SELECT a as b, b as a FROM df")

    expected_df = pd.DataFrame(index=df.index)
    expected_df["b"] = df.a
    expected_df["a"] = df.b

    assert_eq(result_df[["a", "b"]], expected_df[["a", "b"]])


def test_select_column(c, df):
    result_df = c.sql("SELECT a FROM df")

    assert_eq(result_df, df[["a"]])


def test_select_different_types(c):
    expected_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2022-01-21 17:34", "2022-01-21", "17:34", pd.NaT]),
            "string": ["this is a test", "another test", "äölüć", ""],
            "integer": [1, 2, -4, 5],
            "float": [-1.1, np.NaN, pd.NA, np.sqrt(2)],
        }
    )
    c.create_table("df", expected_df)
    result_df = c.sql(
        """
    SELECT *
    FROM df
    """
    )

    assert_eq(result_df, expected_df)


def test_select_expr(c, df):
    result_df = c.sql("SELECT a + 1 AS a, b AS bla, a - 1 FROM df")
    result_df = result_df

    expected_df = pd.DataFrame(
        {"a": df["a"] + 1, "bla": df["b"], '"df"."a" - 1': df["a"] - 1,}
    )
    assert_eq(result_df, expected_df)


def test_select_of_select(c, df):
    result_df = c.sql(
        """
        SELECT 2*c AS e, d - 1 AS f
        FROM
        (
            SELECT a - 1 AS c, 2*b  AS d
            FROM df
        ) AS "inner"
        """
    )

    expected_df = pd.DataFrame({"e": 2 * (df["a"] - 1), "f": 2 * df["b"] - 1})
    assert_eq(result_df, expected_df)


def test_select_of_select_with_casing(c, df):
    result_df = c.sql(
        """
        SELECT AAA, aaa, aAa
        FROM
        (
            SELECT a - 1 AS aAa, 2*b AS aaa, a + b AS AAA
            FROM df
        ) AS "inner"
        """
    )

    expected_df = pd.DataFrame(
        {"AAA": df["a"] + df["b"], "aaa": 2 * df["b"], "aAa": df["a"] - 1}
    )

    assert_eq(result_df, expected_df)


def test_wrong_input(c):
    with pytest.raises(ParsingException):
        c.sql("""SELECT x FROM df""")

    with pytest.raises(ParsingException):
        c.sql("""SELECT x FROM df""")


def test_timezones(c, datetime_table):
    result_df = c.sql(
        """
        SELECT * FROM datetime_table
        """
    )

    assert_eq(result_df, datetime_table)


def test_limit(c, long_table):
    assert_eq(c.sql("SELECT * FROM long_table LIMIT 101"), long_table.iloc[:101])

    assert_eq(c.sql("SELECT * FROM long_table LIMIT 200"), long_table.iloc[:200])

    assert_eq(c.sql("SELECT * FROM long_table LIMIT 100"), long_table.iloc[:100])

    assert_eq(
        c.sql("SELECT * FROM long_table LIMIT 100 OFFSET 99"),
        long_table.iloc[99 : 99 + 100],
    )

    assert_eq(
        c.sql("SELECT * FROM long_table LIMIT 100 OFFSET 100"),
        long_table.iloc[100 : 100 + 100],
    )

    assert_eq(
        c.sql("SELECT * FROM long_table LIMIT 101 OFFSET 101"),
        long_table.iloc[101 : 101 + 101],
    )

    assert_eq(c.sql("SELECT * FROM long_table OFFSET 101"), long_table.iloc[101:])


@pytest.mark.gpu
def test_limit_gpu(c, gpu_long_table):
    df_result = c.sql("SELECT * FROM gpu_long_table LIMIT 101")

    assert_eq(df_result, gpu_long_table.iloc[:101])

    df_result = c.sql("SELECT * FROM gpu_long_table LIMIT 200")

    assert_eq(df_result, gpu_long_table.iloc[:200])

    df_result = c.sql("SELECT * FROM gpu_long_table LIMIT 100")

    assert_eq(df_result, gpu_long_table.iloc[:100])

    df_result = c.sql("SELECT * FROM gpu_long_table LIMIT 100 OFFSET 99")

    assert_eq(df_result, gpu_long_table.iloc[99 : 99 + 100])

    df_result = c.sql("SELECT * FROM gpu_long_table LIMIT 100 OFFSET 100")

    assert_eq(df_result, gpu_long_table.iloc[100 : 100 + 100])

    df_result = c.sql("SELECT * FROM gpu_long_table LIMIT 101 OFFSET 101")

    assert_eq(df_result, gpu_long_table.iloc[101 : 101 + 101])
