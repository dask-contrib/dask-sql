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
    ORDER BY user_id, b
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame({"user_id": [1, 2, 2, 3], "R": [1, 2, 3, 4]})
    expected_df["R"] = expected_df["R"].astype("Int64")
    assert_frame_equal(df.reset_index(drop=True), expected_df.reset_index(drop=True))


def test_over_with_partitioning(c, user_table_2):
    df = c.sql(
        """
    SELECT
        user_id,
        ROW_NUMBER() OVER (PARTITION BY c) AS R
    FROM user_table_2
    ORDER BY user_id, c
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame({"user_id": [1, 1, 2, 4], "R": [1, 1, 1, 1]})
    expected_df["R"] = expected_df["R"].astype("Int64")
    assert_frame_equal(df.reset_index(drop=True), expected_df.reset_index(drop=True))


def test_over_with_grouping_and_sort(c, user_table_1):
    df = c.sql(
        """
    SELECT
        user_id,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY b) AS R
    FROM user_table_1
    ORDER BY user_id, b
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame({"user_id": [1, 2, 2, 3], "R": [1, 1, 2, 1]})
    expected_df["R"] = expected_df["R"].astype("Int64")
    assert_frame_equal(df.reset_index(drop=True), expected_df.reset_index(drop=True))


def test_over_with_different(c, user_table_1):
    df = c.sql(
        """
    SELECT
        user_id,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY b) AS R1,
        ROW_NUMBER() OVER (ORDER BY user_id, b) AS R2
    FROM user_table_1
    ORDER BY user_id, b
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame(
        {"user_id": [1, 2, 2, 3], "R1": [1, 1, 2, 1], "R2": [1, 2, 3, 4],}
    )
    for col in ["R1", "R2"]:
        expected_df[col] = expected_df[col].astype("Int64")
    assert_frame_equal(df.reset_index(drop=True), expected_df.reset_index(drop=True))


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
    ORDER BY user_id, b
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame(
        {
            "O1": [1, 1, 2, 1],
            "O2": [7, 19, 19, 27],
            "O3": [7, 19, 19, 27],
            "O4": [7, 17, 17, 27],
            "O5": [1, 4, 4, 3],
            "O6": [1, 2, 2, 3],
            "O7": [1, 2, 2, 1],
            "O7b": [1, 2, 2, 1],
            "O8": [3, 3, 3, 3],
            "O9": [3, 1, 1, 3],
        }
    )
    for col in expected_df.columns:
        if col in ["06"]:
            continue
        expected_df[col] = expected_df[col].astype("Int64")
    expected_df["O6"] = expected_df["O6"].astype("float64")
    assert_frame_equal(df.reset_index(drop=True), expected_df.reset_index(drop=True))
