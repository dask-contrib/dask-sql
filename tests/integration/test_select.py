import pytest

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import dask.dataframe as dd

from dask_sql.utils import ParsingException


def test_select(c, df):
    result_df = c.sql("SELECT * FROM df")
    result_df = result_df.compute()

    assert_frame_equal(result_df, df)


def test_select_alias(c, df):
    result_df = c.sql("SELECT a as b, b as a FROM df")
    result_df = result_df.compute()

    expected_df = pd.DataFrame(index=df.index)
    expected_df["b"] = df.a
    expected_df["a"] = df.b

    assert_frame_equal(result_df[["a", "b"]], expected_df[["a", "b"]])


def test_select_column(c, df):
    result_df = c.sql("SELECT a FROM df")
    result_df = result_df.compute()

    assert_frame_equal(result_df, df[["a"]])


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
    df = c.sql(
        """
    SELECT *
    FROM df
    """
    )
    df = df.compute()

    assert_frame_equal(df, expected_df)


def test_select_expr(c, df):
    result_df = c.sql("SELECT a + 1 AS a, b AS bla, a - 1 FROM df")
    result_df = result_df.compute()

    expected_df = pd.DataFrame(
        {"a": df["a"] + 1, "bla": df["b"], '"df"."a" - 1': df["a"] - 1,}
    )
    assert_frame_equal(result_df, expected_df)


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
    result_df = result_df.compute()

    expected_df = pd.DataFrame({"e": 2 * (df["a"] - 1), "f": 2 * df["b"] - 1})
    assert_frame_equal(result_df, expected_df)


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
    result_df = result_df.compute()

    expected_df = pd.DataFrame(
        {"AAA": df["a"] + df["b"], "aaa": 2 * df["b"], "aAa": df["a"] - 1}
    )
    assert_frame_equal(result_df, expected_df)


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
    result_df = result_df.compute()

    assert_frame_equal(result_df, datetime_table)
