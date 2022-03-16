import dask.dataframe as dd
import pandas as pd
import pytest
from dask.utils_test import hlg_layer
from pandas.testing import assert_frame_equal

from dask_sql import Context
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


def test_predicate_pushdown_complicated(tmpdir):

    # Write simple parquet dataset
    dd.from_pandas(
        pd.DataFrame({"a": [1, 2, 3] * 5, "b": range(15), "c": ["A"] * 15}),
        npartitions=3,
    ).to_parquet(tmpdir)

    # Read back with dask and apply WHERE query
    ddf = dd.read_parquet(tmpdir)
    df = ddf.compute()

    context = Context()
    context.create_table("my_table", ddf)
    return_df = context.sql("SELECT * FROM my_table WHERE a < 3 AND (b > 1 AND b < 3)")

    # Check for predicate pushdown
    assert hlg_layer(return_df.dask, "read-parquet").creation_info["kwargs"]["filters"]
    return_df = return_df.compute()

    expected_df = df[((df["a"] < 3) & ((df["b"] > 1) & (df["b"] < 3)))]
    assert_frame_equal(
        return_df, expected_df,
    )
