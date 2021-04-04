import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal


def test_group_by(c):
    df = c.sql(
        """
    SELECT
        user_id, SUM(b) AS "S"
    FROM user_table_1
    GROUP BY user_id
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame({"user_id": [1, 2, 3], "S": [3, 4, 3]})
    assert_frame_equal(df.sort_values("user_id").reset_index(drop=True), expected_df)


def test_group_by_all(c, df):
    result_df = c.sql(
        """
    SELECT
        SUM(b) AS "S", SUM(2) AS "X"
    FROM user_table_1
    """
    )
    result_df = result_df.compute()

    expected_df = pd.DataFrame({"S": [10], "X": [8]})
    expected_df["S"] = expected_df["S"].astype("int64")
    expected_df["X"] = expected_df["X"].astype("int32")
    assert_frame_equal(result_df, expected_df)

    result_df = c.sql(
        """
        SELECT
            SUM(a) AS sum_a,
            AVG(a) AS avg_a,
            SUM(b) AS sum_b,
            AVG(b) AS avg_b,
            SUM(a)+AVG(b) AS mix_1,
            SUM(a+b) AS mix_2,
            AVG(a+b) AS mix_3
        FROM df
        """
    )
    result_df = result_df.compute()

    expected_df = pd.DataFrame(
        {
            "sum_a": [df.a.sum()],
            "avg_a": [df.a.mean()],
            "sum_b": [df.b.sum()],
            "avg_b": [df.b.mean()],
            "mix_1": [df.a.sum() + df.b.mean()],
            "mix_2": [(df.a + df.b).sum()],
            "mix_3": [(df.a + df.b).mean()],
        }
    )
    assert_frame_equal(result_df, expected_df)


def test_group_by_filtered(c):
    df = c.sql(
        """
    SELECT
        SUM(b) FILTER (WHERE user_id = 2) AS "S1",
        SUM(b) "S2"
    FROM user_table_1
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame({"S1": [4], "S2": [10]}, dtype="int64")
    assert_frame_equal(df, expected_df)


def test_group_by_filtered2(c):
    df = c.sql(
        """
    SELECT
        user_id,
        SUM(b) FILTER (WHERE user_id = 2) AS "S1",
        SUM(b) "S2"
    FROM user_table_1
    GROUP BY user_id
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame(
        {"user_id": [1, 2, 3], "S1": [np.NaN, 4.0, np.NaN], "S2": [3, 4, 3],},
    )
    assert_frame_equal(df, expected_df)

    df = c.sql(
        """
    SELECT
        SUM(b) FILTER (WHERE user_id = 2) AS "S1"
    FROM user_table_1
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame({"S1": [4]})
    assert_frame_equal(df, expected_df)


def test_group_by_case(c):
    df = c.sql(
        """
    SELECT
        user_id + 1, SUM(CASE WHEN b = 3 THEN 1 END) AS "S"
    FROM user_table_1
    GROUP BY user_id + 1
    """
    )
    df = df.compute()

    user_id_column = '"user_table_1"."user_id" + 1'

    expected_df = pd.DataFrame({user_id_column: [2, 3, 4], "S": [1, 1, 1]})
    expected_df[user_id_column] = expected_df[user_id_column].astype("int64")
    assert_frame_equal(
        df.sort_values(user_id_column).reset_index(drop=True), expected_df
    )


def test_group_by_nan(c):
    df = c.sql(
        """
    SELECT
        c
    FROM user_table_nan
    GROUP BY c
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame({"c": [3, float("nan"), 1]})
    # The dtype in pandas 1.0.5 and pandas 1.1.0 are different, so
    # we can not check here
    assert_frame_equal(df, expected_df, check_dtype=False)

    df = c.sql(
        """
    SELECT
        c
    FROM user_table_inf
    GROUP BY c
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame({"c": [3, 1, float("inf")]})
    expected_df["c"] = expected_df["c"].astype("float64")
    assert_frame_equal(df, expected_df)


def test_aggregations(c):
    df = c.sql(
        """
    SELECT
        user_id,
        EVERY(b = 3) AS e,
        BIT_AND(b) AS b,
        BIT_OR(b) AS bb,
        MIN(b) AS m,
        SINGLE_VALUE(b) AS s,
        AVG(b) AS a
    FROM user_table_1
    GROUP BY user_id
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame(
        {
            "user_id": [1, 2, 3],
            "e": [True, False, True],
            "b": [3, 1, 3],
            "bb": [3, 3, 3],
            "m": [3, 1, 3],
            "s": [3, 3, 3],
            "a": [3, 2, 3],
        }
    )
    expected_df["a"] = expected_df["a"].astype("float64")
    assert_frame_equal(df.sort_values("user_id").reset_index(drop=True), expected_df)

    df = c.sql(
        """
    SELECT
        user_id,
        EVERY(c = 3) AS e,
        BIT_AND(c) AS b,
        BIT_OR(c) AS bb,
        MIN(c) AS m,
        SINGLE_VALUE(c) AS s,
        AVG(c) AS a
    FROM user_table_2
    GROUP BY user_id
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame(
        {
            "user_id": [1, 2, 4],
            "e": [False, True, False],
            "b": [0, 3, 4],
            "bb": [3, 3, 4],
            "m": [1, 3, 4],
            "s": [1, 3, 4],
            "a": [1.5, 3, 4],
        }
    )
    assert_frame_equal(df.sort_values("user_id").reset_index(drop=True), expected_df)

    df = c.sql(
        """
    SELECT
        MAX(a) AS "max",
        MIN(a) AS "min"
    FROM string_table
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame({"max": ["a normal string"], "min": ["%_%"]})
    assert_frame_equal(df.reset_index(drop=True), expected_df)
