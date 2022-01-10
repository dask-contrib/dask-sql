import pandas as pd
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


def test_filter_datetime(c):
    df = pd.DataFrame({"year": [2015, 2016], "month": [2, 3], "day": [4, 5]})

    df["dt"] = pd.to_datetime(df)

    c.create_table("datetime_test", df)
    actual_df = c.sql("select * from datetime_test where year(dt) < 2016").compute()
    expected_df = df[df["year"] < 2016]

    assert_frame_equal(expected_df, actual_df)
