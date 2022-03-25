import dask.dataframe as dd
import pandas as pd
import pytest
from dask.utils_test import hlg_layer
from pandas.testing import assert_frame_equal

from dask_sql._compat import INT_NAN_IMPLEMENTED


def test_filter(c, df):
    return_df = c.sql("SELECT * FROM df WHERE a < 2")
    return_df = return_df.compute()

    expected_df = df[df["a"] < 2]
    assert_frame_equal(return_df, expected_df)


def test_filter_scalar(c, df):
    return_df = c.sql("SELECT * FROM df WHERE True")
    return_df = return_df.compute()

    expected_df = df
    assert_frame_equal(return_df, expected_df)

    return_df = c.sql("SELECT * FROM df WHERE False")
    return_df = return_df.compute()

    expected_df = df.head(0)
    assert_frame_equal(return_df, expected_df, check_index_type=False)

    return_df = c.sql("SELECT * FROM df WHERE (1 = 1)")
    return_df = return_df.compute()

    expected_df = df
    assert_frame_equal(return_df, expected_df)

    return_df = c.sql("SELECT * FROM df WHERE (1 = 0)")
    return_df = return_df.compute()

    expected_df = df.head(0)
    assert_frame_equal(return_df, expected_df, check_index_type=False)


def test_filter_complicated(c, df):
    return_df = c.sql("SELECT * FROM df WHERE a < 3 AND (b > 1 AND b < 3)")
    return_df = return_df.compute()

    expected_df = df[((df["a"] < 3) & ((df["b"] > 1) & (df["b"] < 3)))]
    assert_frame_equal(
        return_df, expected_df,
    )


def test_filter_with_nan(c):
    return_df = c.sql("SELECT * FROM user_table_nan WHERE c = 3")
    return_df = return_df.compute()

    if INT_NAN_IMPLEMENTED:
        expected_df = pd.DataFrame({"c": [3]}, dtype="Int8")
    else:
        expected_df = pd.DataFrame({"c": [3]}, dtype="float")
    assert_frame_equal(
        return_df, expected_df,
    )


def test_string_filter(c, string_table):
    return_df = c.sql("SELECT * FROM string_table WHERE a = 'a normal string'")
    return_df = return_df.compute()

    assert_frame_equal(
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

    assert_frame_equal(expected_df, actual_df)


@pytest.mark.parametrize(
    "query,df_func,filters",
    [
        (
            "SELECT * FROM parquet_ddf WHERE b < 10",
            lambda x: x[x["b"] < 10],
            [[("b", "<", 10)]],
        ),
        (
            "SELECT * FROM parquet_ddf WHERE a < 3 AND (b > 1 AND b < 5)",
            lambda x: x[(x["a"] < 3) & ((x["b"] > 1) & (x["b"] < 5))],
            [[("a", "<", 3), ("b", ">", 1), ("b", "<", 5)]],
        ),
        (
            "SELECT * FROM parquet_ddf WHERE (b > 5 AND b < 10) OR a = 1",
            lambda x: x[((x["b"] > 5) & (x["b"] < 10)) | (x["a"] == 1)],
            [[("a", "==", 1)], [("b", "<", 10), ("b", ">", 5)]],
        ),
        (
            "SELECT * FROM parquet_ddf WHERE b IN (1, 6)",
            lambda x: x[(x["b"] == 1) | (x["b"] == 6)],
            [[("b", "<=", 1), ("b", ">=", 1)], [("b", "<=", 6), ("b", ">=", 6)]],
        ),
        (
            "SELECT a FROM parquet_ddf WHERE (b > 5 AND b < 10) OR a = 1",
            lambda x: x[((x["b"] > 5) & (x["b"] < 10)) | (x["a"] == 1)][["a"]],
            [[("a", "==", 1)], [("b", "<", 10), ("b", ">", 5)]],
        ),
        (
            # Original filters NOT in disjunctive normal form
            "SELECT a FROM parquet_ddf WHERE (parquet_ddf.b > 3 AND parquet_ddf.b < 10 OR parquet_ddf.a = 1) AND (parquet_ddf.c = 'A')",
            lambda x: x[
                ((x["b"] > 3) & (x["b"] < 10) | (x["a"] == 1)) & (x["c"] == "A")
            ][["a"]],
            [
                [("c", "==", "A"), ("b", ">", 3), ("b", "<", 10)],
                [("a", "==", 1), ("c", "==", "A")],
            ],
        ),
        (
            # The predicate-pushdown optimization will be skipped here,
            # because datetime accessors are not supported. However,
            # the query should still succeed.
            "SELECT * FROM parquet_ddf WHERE year(d) < 2015",
            lambda x: x[x["d"].dt.year < 2015],
            None,
        ),
    ],
)
def test_predicate_pushdown(c_parquet, parquet_ddf, query, df_func, filters):
    # Check for predicate pushdown.
    # We can use the `hlg_layer` utility to make sure the
    # `filters` field has been populated in `creation_info`
    return_df = c_parquet.sql(query)
    expect_filters = filters
    got_filters = hlg_layer(return_df.dask, "read-parquet").creation_info["kwargs"][
        "filters"
    ]
    if expect_filters:
        got_filters = frozenset(frozenset(v) for v in got_filters)
        expect_filters = frozenset(frozenset(v) for v in filters)
    assert got_filters == expect_filters

    # Check computed result is correct
    df = parquet_ddf.compute()
    expected_df = df_func(df)
    dd.assert_eq(return_df, expected_df)


def test_filtered_csv(tmpdir, c):
    # Predicate pushdown is NOT supported for CSV data.
    # This test just checks that the "attempted"
    # predicate-pushdown logic does not lead to
    # any unexpected errors

    # Write simple csv dataset
    df = pd.DataFrame({"a": [1, 2, 3] * 5, "b": range(15), "c": ["A"] * 15,},)
    dd.from_pandas(df, npartitions=3).to_csv(tmpdir + "/*.csv", index=False)

    # Read back with dask and apply WHERE query
    csv_ddf = dd.read_csv(tmpdir + "/*.csv")
    try:
        c.create_table("my_csv_table", csv_ddf)
        return_df = c.sql("SELECT * FROM my_csv_table WHERE b < 10")
    finally:
        c.drop_table("my_csv_table")

    # Check computed result is correct
    df = csv_ddf.compute()
    expected_df = df[df["b"] < 10]
    dd.assert_eq(return_df, expected_df)
