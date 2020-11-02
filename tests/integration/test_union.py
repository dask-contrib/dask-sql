import pandas as pd
from pandas.testing import assert_frame_equal


def test_union_not_all(c, df):
    result_df = c.sql(
        """
        SELECT * FROM df
        UNION
        SELECT * FROM df
        UNION
        SELECT * FROM df
        """
    )
    result_df = result_df.compute()

    assert_frame_equal(result_df.reset_index(drop=True), df)


def test_union_all(c, df):
    result_df = c.sql(
        """
        SELECT * FROM df
        UNION ALL
        SELECT * FROM df
        UNION ALL
        SELECT * FROM df
        """
    )
    result_df = result_df.compute()

    expected_df = pd.concat([df, df, df], ignore_index=True)
    assert_frame_equal(result_df.reset_index(drop=True), expected_df)


def test_union_mixed(c, df, long_table):
    result_df = c.sql(
        """
        SELECT a AS "I", b as "II" FROM df
        UNION ALL
        SELECT a as "I", a as "II" FROM long_table
        """
    )
    result_df = result_df.compute()

    long_table = long_table.rename(columns={"a": "I"})
    long_table["II"] = long_table["I"]

    expected_df = pd.concat(
        [df.rename(columns={"a": "I", "b": "II"}), long_table], ignore_index=True,
    )
    assert_frame_equal(result_df.reset_index(drop=True), expected_df)
