import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal


def test_over_with_sorting(c, user_table_1):
    df = c.sql(
        """
    SELECT
        user_id,
        ROW_NUMBER() OVER (ORDER BY user_id, b) AS R
    FROM user_table_1
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame({"user_id": user_table_1.user_id, "R": [3, 1, 2, 4]})
    expected_df["R"] = expected_df["R"].astype("Int64")
    assert_frame_equal(df, expected_df)


def test_over_with_partitioning(c, user_table_2):
    df = c.sql(
        """
    SELECT
        user_id,
        ROW_NUMBER() OVER (PARTITION BY c) AS R
    FROM user_table_2
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame({"user_id": user_table_2.user_id, "R": [1, 1, 1, 1]})
    expected_df["R"] = expected_df["R"].astype("Int64")
    assert_frame_equal(df, expected_df)


def test_over_with_grouping_and_sort(c, user_table_1):
    df = c.sql(
        """
    SELECT
        user_id,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY b) AS R
    FROM user_table_1
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame({"user_id": user_table_1.user_id, "R": [2, 1, 1, 1]})
    expected_df["R"] = expected_df["R"].astype("Int64")
    assert_frame_equal(df, expected_df)


def test_over_with_different(c, user_table_1):
    df = c.sql(
        """
    SELECT
        user_id,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY b) AS R1,
        ROW_NUMBER() OVER (ORDER BY user_id, b) AS R2
    FROM user_table_1
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame(
        {"user_id": user_table_1.user_id, "R1": [2, 1, 1, 1], "R2": [3, 1, 2, 4],}
    )
    for col in ["R1", "R2"]:
        expected_df[col] = expected_df[col].astype("Int64")
    assert_frame_equal(df, expected_df)


def test_over_calls(c):
    df = c.sql(
        """
    SELECT
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY b) AS O1,
        FIRST_VALUE(user_id*10 - b) OVER (PARTITION BY user_id ORDER BY b) AS O2,
        SINGLE_VALUE(user_id*10 - b) OVER (PARTITION BY user_id ORDER BY b) AS O3,
        LAST_VALUE(user_id*10 - b) OVER (PARTITION BY user_id ORDER BY b) AS O4,
        SUM(user_id) OVER (PARTITION BY user_id ORDER BY b) AS O5,
        AVG(user_id) OVER (PARTITION BY user_id ORDER BY b) AS O6,
        COUNT(*) OVER (PARTITION BY user_id ORDER BY b) AS O7,
        COUNT(b) OVER (PARTITION BY user_id ORDER BY b) AS O7b,
        MAX(b) OVER (PARTITION BY user_id ORDER BY b) AS O8,
        MIN(b) OVER (PARTITION BY user_id ORDER BY b) AS O9
    FROM user_table_1
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame(
        {
            "O1": [2, 1, 1, 1],
            "O2": [19, 7, 19, 27],
            "O3": [19, 7, 19, 27],
            "O4": [17, 7, 17, 27],
            "O5": [4, 1, 4, 3],
            "O6": [2, 1, 2, 3],
            "O7": [2, 1, 2, 1],
            "O7b": [2, 1, 2, 1],
            "O8": [3, 3, 3, 3],
            "O9": [1, 3, 1, 3],
        }
    )
    for col in expected_df.columns:
        if col in ["06"]:
            continue
        expected_df[col] = expected_df[col].astype("Int64")
    expected_df["O6"] = expected_df["O6"].astype("float64")
    assert_frame_equal(df, expected_df)
