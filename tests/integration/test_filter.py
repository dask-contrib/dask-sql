import dask
import dask.dataframe as dd
import pandas as pd
import pytest
from dask.utils_test import hlg_layer
from packaging.version import parse as parseVersion

from dask_sql._compat import PQ_IS_SUPPORT, PQ_NOT_IN_SUPPORT
from tests.utils import assert_eq

DASK_GT_2022_4_2 = parseVersion(dask.__version__) >= parseVersion("2022.4.2")


def test_filter(c, df):
    return_df = c.sql("SELECT * FROM df WHERE a < 2")

    expected_df = df[df["a"] < 2]
    assert_eq(return_df, expected_df)


def test_filter_scalar(c, df):
    return_df = c.sql("SELECT * FROM df WHERE True")

    expected_df = df
    assert_eq(return_df, expected_df)

    return_df = c.sql("SELECT * FROM df WHERE False")

    expected_df = df.head(0)
    assert_eq(return_df, expected_df, check_index_type=False)

    return_df = c.sql("SELECT * FROM df WHERE (1 = 1)")

    expected_df = df
    assert_eq(return_df, expected_df)

    return_df = c.sql("SELECT * FROM df WHERE (1 = 0)")

    expected_df = df.head(0)
    assert_eq(return_df, expected_df, check_index_type=False)


def test_filter_complicated(c, df):
    return_df = c.sql("SELECT * FROM df WHERE a < 3 AND (b > 1 AND b < 3)")

    expected_df = df[((df["a"] < 3) & ((df["b"] > 1) & (df["b"] < 3)))]
    assert_eq(
        return_df,
        expected_df,
    )


def test_filter_with_nan(c):
    return_df = c.sql("SELECT * FROM user_table_nan WHERE c = 3")
    expected_df = pd.DataFrame({"c": [3]}, dtype="Int8")

    assert_eq(
        return_df,
        expected_df,
    )


def test_string_filter(c, string_table):
    return_df = c.sql("SELECT * FROM string_table WHERE a = 'a normal string'")

    assert_eq(
        return_df,
        string_table.head(1),
    )
    # Condition needs to specifically check on `M` since this the literal `M`
    # was getting parsed as a datetime dtype
    return_df = c.sql("SELECT * from string_table WHERE a = 'M'")
    expected_df = string_table[string_table["a"] == "M"]
    assert_eq(return_df, expected_df)


@pytest.mark.parametrize(
    "input_table",
    [
        "datetime_table",
        pytest.param(
            "gpu_datetime_table",
            marks=(pytest.mark.gpu),
        ),
    ],
)
def test_filter_cast_date(c, input_table, request):
    datetime_table = request.getfixturevalue(input_table)
    return_df = c.sql(
        f"""
        SELECT * FROM {input_table} WHERE
            CAST(timezone AS DATE) > DATE '2014-08-01'
        """
    )

    # FIXME: dt.tz_localize(None) fails on timezone-naive cuDF series
    # https://github.com/rapidsai/cudf/issues/13601
    delocalized_col = (
        datetime_table["timezone"].dt.tz_localize(None)
        if "gpu" not in input_table
        else datetime_table["timezone"]
    )

    expected_df = datetime_table[
        delocalized_col.dt.floor("D").astype("<M8[ns]") > pd.Timestamp("2014-08-01")
    ]
    assert_eq(return_df, expected_df)


@pytest.mark.parametrize(
    "input_table",
    [
        pytest.param(
            "datetime_table",
            marks=pytest.mark.xfail(
                reason="Need support for non-UTC timezoned literals"
            ),
        ),
        pytest.param(
            "gpu_datetime_table",
            marks=(pytest.mark.gpu),
        ),
    ],
)
def test_filter_cast_timestamp(c, input_table, request):
    datetime_table = request.getfixturevalue(input_table)
    return_df = c.sql(
        f"""
        SELECT * FROM {input_table} WHERE
            CAST(timezone AS TIMESTAMP) >= TIMESTAMP '2014-08-01 23:00:00+00'
        """
    )

    expected_df = datetime_table[
        datetime_table["timezone"].astype("<M8[ns]")
        >= pd.Timestamp("2014-08-01 23:00:00")
    ]
    assert_eq(return_df, expected_df)


def test_filter_year(c):
    df = pd.DataFrame({"year": [2015, 2016], "month": [2, 3], "day": [4, 5]})
    df["dt"] = pd.to_datetime(df)

    c.create_table("datetime_test", df)

    return_df = c.sql("select * from datetime_test where year(dt) < 2016")
    expected_df = df[df["year"] < 2016]

    assert_eq(expected_df, return_df)


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
            [[("b", ">", 5), ("b", "<", 10)], [("a", "==", 1)]],
        ),
        pytest.param(
            "SELECT * FROM parquet_ddf WHERE b IN (1, 6)",
            lambda x: x[(x["b"] == 1) | (x["b"] == 6)],
            [[("b", "==", 1)], [("b", "==", 6)]],
        ),
        pytest.param(
            "SELECT * FROM parquet_ddf WHERE b IN (1, 3, 5, 6)",
            lambda x: x[x["b"].isin([1, 3, 5, 6])],
            [[("b", "in", (1, 3, 5, 6))]],
        ),
        pytest.param(
            "SELECT * FROM parquet_ddf WHERE c IN ('A', 'B', 'C', 'D')",
            lambda x: x[x["c"].isin(["A", "B", "C", "D"])],
            [[("c", "in", ("A", "B", "C", "D"))]],
        ),
        pytest.param(
            "SELECT * FROM parquet_ddf WHERE b NOT IN (1, 6)",
            lambda x: x[(x["b"] != 1) & (x["b"] != 6)],
            [[("b", "!=", 1), ("b", "!=", 6)]],
        ),
        pytest.param(
            "SELECT * FROM parquet_ddf WHERE b NOT IN (1, 3, 5, 6)",
            lambda x: x[~x["b"].isin([1, 3, 5, 6])],
            [[("b", "not in", (1, 3, 5, 6))]],
            marks=pytest.mark.skipif(
                not PQ_NOT_IN_SUPPORT,
                reason="Requires https://github.com/dask/dask/pull/10320",
            ),
        ),
        (
            "SELECT a FROM parquet_ddf WHERE (b > 5 AND b < 10) OR a = 1",
            lambda x: x[((x["b"] > 5) & (x["b"] < 10)) | (x["a"] == 1)][["a"]],
            [[("b", ">", 5), ("b", "<", 10)], [("a", "==", 1)]],
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
def test_predicate_pushdown(c, parquet_ddf, query, df_func, filters):

    # Check for predicate pushdown.
    # We can use the `hlg_layer` utility to make sure the
    # `filters` field has been populated in `creation_info`
    return_df = c.sql(query)
    expect_filters = filters
    got_filters = hlg_layer(return_df.dask, "read-parquet").creation_info["kwargs"][
        "filters"
    ]
    if expect_filters:
        got_filters = frozenset(frozenset(v) for v in got_filters)
        expect_filters = frozenset(frozenset(v) for v in filters)

    assert got_filters == expect_filters

    # Check computed result is correct
    df = parquet_ddf
    expected_df = df_func(df)

    # divisions aren't equal for older dask versions
    assert_eq(
        return_df, expected_df, check_index=False, check_divisions=DASK_GT_2022_4_2
    )


def test_filtered_csv(tmpdir, c):
    # Predicate pushdown is NOT supported for CSV data.
    # This test just checks that the "attempted"
    # predicate-pushdown logic does not lead to
    # any unexpected errors

    # Write simple csv dataset
    df = pd.DataFrame(
        {
            "a": [1, 2, 3] * 5,
            "b": range(15),
            "c": ["A"] * 15,
        },
    )
    dd.from_pandas(df, npartitions=3).to_csv(tmpdir + "/*.csv", index=False)

    # Read back with dask and apply WHERE query
    csv_ddf = dd.read_csv(tmpdir + "/*.csv")
    try:
        c.create_table("my_csv_table", csv_ddf)
        return_df = c.sql("SELECT * FROM my_csv_table WHERE b < 10")
    finally:
        c.drop_table("my_csv_table")

    # Check computed result is correct
    df = csv_ddf
    expected_df = df[df["b"] < 10]

    assert_eq(return_df, expected_df)


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_filter_decimal(c, gpu):
    df = pd.DataFrame(
        {
            "a": [304.5, 35.305, 9.043, 102.424, 53.34],
            "b": [2.2, 82.4, 42, 76.9, 54.4],
            "c": [1, 2, 2, 5, 9],
        }
    )
    c.create_table("df", df, gpu=gpu)

    result_df = c.sql(
        """
        SELECT
            c
        FROM
            df
        WHERE
            CAST(a AS DECIMAL) < CAST(b AS DECIMAL)
        """
    )

    expected_df = df.loc[df.a < df.b][["c"]]

    assert_eq(result_df, expected_df)

    result_df = c.sql(
        """
        SELECT
            CAST(b AS DECIMAL) as b
        FROM
            df
        WHERE
            CAST(a AS DECIMAL) < DECIMAL '100.2'
        """
    )

    # decimal precision doesn't match up with pandas floats
    if gpu:
        result_df["b"] = result_df["b"].astype("float64")

    expected_df = df.loc[df.a < 100.2][["b"]]

    assert_eq(result_df, expected_df, check_index=False)
    c.drop_table("df")


@pytest.mark.skipif(
    not PQ_IS_SUPPORT,
    reason="Requires https://github.com/dask/dask/pull/10320",
)
def test_predicate_pushdown_isna(tmpdir):
    from dask_sql.context import Context

    c = Context()

    path = str(tmpdir)
    dd.from_pandas(
        pd.DataFrame(
            {
                "a": [1, 2, None] * 5,
                "b": range(15),
                "index": range(15),
            }
        ),
        npartitions=3,
    ).to_parquet(path + "/df1")
    df1 = dd.read_parquet(path + "/df1", index="index")
    c.create_table("df1", df1)

    dd.from_pandas(
        pd.DataFrame(
            {
                "a": [None, 2, 3] * 5,
                "b": range(15),
                "index": range(15),
            },
        ),
        npartitions=3,
    ).to_parquet(path + "/df2")
    df2 = dd.read_parquet(path + "/df2", index="index")
    c.create_table("df2", df2)

    return_df = c.sql("SELECT df1.a FROM df1, df2 WHERE df1.a = df2.a")

    # Check for predicate pushdown
    filters = [[("a", "is not", None)]]
    got_filters = hlg_layer(return_df.dask, "read-parquet").creation_info["kwargs"][
        "filters"
    ]

    got_filters = frozenset(frozenset(v) for v in got_filters)
    expect_filters = frozenset(frozenset(v) for v in filters)

    assert got_filters == expect_filters
    assert all(return_df.compute() == 2)
    assert len(return_df) == 25
