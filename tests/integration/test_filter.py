import dask.dataframe as dd
import pandas as pd
import pytest
from dask.utils_test import hlg_layer

from dask_sql._compat import INT_NAN_IMPLEMENTED


def test_filter(c, df):
    return_df = c.sql("SELECT * FROM df WHERE a < 2")
    return_df = return_df.compute()

    expected_df = df[df["a"] < 2]
    dd.assert_eq(return_df, expected_df)


def test_filter_scalar(c, df):
    return_df = c.sql("SELECT * FROM df WHERE True")
    return_df = return_df.compute()

    expected_df = df
    dd.assert_eq(return_df, expected_df)

    return_df = c.sql("SELECT * FROM df WHERE False")
    return_df = return_df.compute()

    expected_df = df.head(0)
    dd.assert_eq(return_df, expected_df, check_index_type=False)

    return_df = c.sql("SELECT * FROM df WHERE (1 = 1)")
    return_df = return_df.compute()

    expected_df = df
    dd.assert_eq(return_df, expected_df)

    return_df = c.sql("SELECT * FROM df WHERE (1 = 0)")
    return_df = return_df.compute()

    expected_df = df.head(0)
    dd.assert_eq(return_df, expected_df, check_index_type=False)


def test_filter_complicated(c, df):
    return_df = c.sql("SELECT * FROM df WHERE a < 3 AND (b > 1 AND b < 3)")
    return_df = return_df.compute()

    expected_df = df[((df["a"] < 3) & ((df["b"] > 1) & (df["b"] < 3)))]
    dd.assert_eq(
        return_df, expected_df,
    )


def test_filter_with_nan(c):
    return_df = c.sql("SELECT * FROM user_table_nan WHERE c = 3")
    return_df = return_df.compute()

    if INT_NAN_IMPLEMENTED:
        expected_df = pd.DataFrame({"c": [3]}, dtype="Int8")
    else:
        expected_df = pd.DataFrame({"c": [3]}, dtype="float")
    dd.assert_eq(
        return_df, expected_df,
    )


def test_string_filter(c, string_table):
    return_df = c.sql("SELECT * FROM string_table WHERE a = 'a normal string'")
    return_df = return_df.compute()

    dd.assert_eq(
        return_df, string_table.head(1),
    )


@pytest.mark.parametrize(
    "input_table",
    ["datetime_table", pytest.param("gpu_datetime_table", marks=pytest.mark.gpu),],
)
def test_filter_cast_date(c, input_table, request):
    datetime_table = request.getfixturevalue(input_table)
    return_df = c.sql(
        f"""
        SELECT * FROM {input_table} WHERE
            CAST(timezone AS DATE) > DATE '2014-08-01'
        """
    )

    expected_df = datetime_table[
        datetime_table["timezone"].astype("<M8[ns]").dt.floor("D").astype("<M8[ns]")
        > pd.Timestamp("2014-08-01")
    ]
    dd.assert_eq(return_df, expected_df)


@pytest.mark.parametrize(
    "input_table",
    ["datetime_table", pytest.param("gpu_datetime_table", marks=pytest.mark.gpu),],
)
def test_filter_cast_timestamp(c, input_table, request):
    datetime_table = request.getfixturevalue(input_table)
    return_df = c.sql(
        f"""
        SELECT * FROM {input_table} WHERE
            CAST(timezone AS TIMESTAMP) >= TIMESTAMP '2014-08-01 23:00:00'
        """
    )

    expected_df = datetime_table[
        datetime_table["timezone"].astype("<M8[ns]")
        >= pd.Timestamp("2014-08-01 23:00:00")
    ]
    dd.assert_eq(return_df, expected_df)


def test_filter_year(c):
    df = pd.DataFrame({"year": [2015, 2016], "month": [2, 3], "day": [4, 5]})

    df["dt"] = pd.to_datetime(df)

    c.create_table("datetime_test", df)
    actual_df = c.sql("select * from datetime_test where year(dt) < 2016").compute()
    expected_df = df[df["year"] < 2016]

    dd.assert_eq(expected_df, actual_df)


@pytest.mark.parametrize(
    "query,df_func",
    [
        ("SELECT * FROM parquet_ddf WHERE b < 10", lambda x: x[x["b"] < 10]),
        (
            "SELECT * FROM parquet_ddf WHERE a < 3 AND (b > 1 AND b < 5)",
            lambda x: x[(x["a"] < 3) & ((x["b"] > 1) & (x["b"] < 5))],
        ),
        (
            "SELECT * FROM parquet_ddf WHERE (b > 5 AND b < 10) OR a = 1",
            lambda x: x[((x["b"] > 5) & (x["b"] < 10)) | (x["a"] == 1)],
        ),
        (
            "SELECT a FROM parquet_ddf WHERE (b > 5 AND b < 10) OR a = 1",
            lambda x: x[((x["b"] > 5) & (x["b"] < 10)) | (x["a"] == 1)][["a"]],
        ),
        (
            # Original filters NOT in disjunctive normal form
            "SELECT a FROM parquet_ddf WHERE (parquet_ddf.b > 3 AND parquet_ddf.b < 10 OR parquet_ddf.a = 1) AND (parquet_ddf.c = 'A')",
            lambda x: x[
                ((x["b"] > 3) & (x["b"] < 10) | (x["a"] == 1)) & (x["c"] == "A")
            ][["a"]],
        ),
        pytest.param(
            "SELECT * FROM parquet_ddf WHERE year(d) < 2015",
            lambda x: x[x["d"].dt.year < 2015],
            # This test will fail, because the filters will not get pushed
            # down to read_parquet. However, the query should still succeed.
            marks=pytest.mark.xfail(
                reason="Predicate pushdown does not support datetime accessors."
            ),
        ),
    ],
)
def test_predicate_pushdown(c, parquet_ddf, query, df_func):

    # Check for predicate pushdown.
    # We can use the `hlg_layer` utility to make sure the
    # `filters` field has been populated in `creation_info`
    return_df = c.sql(query)
    assert hlg_layer(return_df.dask, "read-parquet").creation_info["kwargs"]["filters"]

    # Check computed result is correct
    df = parquet_ddf.compute()
    expected_df = df_func(df)
    dd.assert_eq(return_df, expected_df)
