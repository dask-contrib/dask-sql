from pandas.testing import assert_frame_equal


def test_filter(c, df):
    return_df = c.sql("SELECT * FROM df WHERE a < 2")
    return_df = return_df.compute()

    expected_df = df[df["a"] < 2]
    assert_frame_equal(return_df, expected_df)


def test_filter_complicated(c, df):
    return_df = c.sql("SELECT * FROM df WHERE a < 3 AND (b > 1 AND b < 3)")
    return_df = return_df.compute()

    expected_df = df[((df["a"] < 3) & ((df["b"] > 1) & (df["b"] < 3)))]
    assert_frame_equal(
        return_df, expected_df,
    )
