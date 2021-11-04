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
        expected_df = pd.DataFrame({"c": [3]}, dtype="int8")
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


def test_datetime_filter(c):
    df = pd.DataFrame(
        {"d_date": ["2001-08-01", "2001-08-02", "2001-08-03"], "val": [1, 2, 3]}
    )
    c.create_table("datetime_tbl1", df)
    query = "SELECT val, d_date FROM datetime_tbl1 WHERE CAST(d_date as date) IN (date '2001-08-01', date '2001-08-03')"
    result_df = c.sql(query).compute().reset_index(drop=True)
    expected_df = pd.DataFrame({"val": [1, 3], "d_date": ["2001-08-01", "2001-08-03"]})
    assert_frame_equal(result_df, expected_df, check_dtype=False)
