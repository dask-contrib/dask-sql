import pandas as pd
from dask.dataframe.utils import assert_eq


def test_over_with_sorting(c, user_table_1):
    return_df = c.sql(
        """
    SELECT
        user_id,
        b,
        ROW_NUMBER() OVER (ORDER BY user_id, b) AS R
    FROM user_table_1
    """
    )
    expected_df = user_table_1.sort_values(["user_id", "b"])
    expected_df["R"] = pd.Series([3, 1, 2, 4], dtype="Int64")

    assert_eq(return_df, expected_df, check_index=False)


def test_over_with_partitioning(c, user_table_2):
    return_df = c.sql(
        """
    SELECT
        user_id,
        c,
        ROW_NUMBER() OVER (PARTITION BY c) AS R
    FROM user_table_2
    ORDER BY user_id, c
    """
    )
    expected_df = user_table_2.sort_values(["user_id", "c"])
    expected_df["R"] = pd.Series([1, 1, 1, 1], dtype="Int64")

    assert_eq(return_df, expected_df, check_index=False)


def test_over_with_grouping_and_sort(c, user_table_1):
    return_df = c.sql(
        """
    SELECT
        user_id,
        b,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY b) AS R
    FROM user_table_1
    """
    )
    expected_df = user_table_1.sort_values(["user_id", "b"])
    expected_df["R"] = pd.Series([2, 1, 1, 1], dtype="Int64")

    assert_eq(return_df, expected_df, check_index=False)


def test_over_with_different(c, user_table_1):
    return_df = c.sql(
        """
    SELECT
        user_id,
        b,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY b) AS R1,
        ROW_NUMBER() OVER (ORDER BY user_id, b) AS R2
    FROM user_table_1
    """
    )
    expected_df = pd.DataFrame(
        {
            "user_id": user_table_1.user_id,
            "b": user_table_1.b,
            "R1": pd.Series([2, 1, 1, 1], dtype="Int64"),
            "R2": pd.Series([3, 1, 2, 4], dtype="Int64"),
        }
    )

    assert_eq(return_df, expected_df, check_index=False)


def test_over_calls(c, user_table_1):
    return_df = c.sql(
        """
    SELECT
        user_id,
        b,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY b) AS O1,
        FIRST_VALUE(user_id*10 - b) OVER (PARTITION BY user_id ORDER BY b) AS O2,
        SINGLE_VALUE(user_id*10 - b) OVER (PARTITION BY user_id ORDER BY b) AS O3,
        LAST_VALUE(user_id*10 - b) OVER (PARTITION BY user_id ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS O4,
        SUM(user_id) OVER (PARTITION BY user_id ORDER BY b) AS O5,
        AVG(user_id) OVER (PARTITION BY user_id ORDER BY b) AS O6,
        COUNT(*) OVER (PARTITION BY user_id ORDER BY b) AS O7,
        COUNT(b) OVER (PARTITION BY user_id ORDER BY b) AS O7b,
        MAX(b) OVER (PARTITION BY user_id ORDER BY b) AS O8,
        MIN(b) OVER (PARTITION BY user_id ORDER BY b) AS O9
    FROM user_table_1
    """
    )
    expected_df = pd.DataFrame(
        {
            "user_id": user_table_1.user_id,
            "b": user_table_1.b,
            "O1": pd.Series([2, 1, 1, 1], dtype="Int64"),
            "O2": pd.Series([19, 7, 19, 27], dtype="Int64"),
            "O3": pd.Series([19, 7, 19, 27], dtype="Int64"),
            "O4": pd.Series([17, 7, 17, 27], dtype="Int64"),
            "O5": pd.Series([4, 1, 2, 3], dtype="Int64"),
            "O6": pd.Series([2, 1, 2, 3], dtype="Float64"),
            "O7": pd.Series([2, 1, 1, 1], dtype="Int64"),
            "O7b": pd.Series([2, 1, 1, 1], dtype="Int64"),
            "O8": pd.Series([3, 3, 1, 3], dtype="Int64"),
            "O9": pd.Series([1, 3, 1, 3], dtype="Int64"),
        }
    )

    assert_eq(return_df, expected_df, check_index=False)


def test_over_with_windows(c):
    tmp_df = pd.DataFrame({"a": range(5)})
    c.create_table("tmp", tmp_df)

    return_df = c.sql(
        """
    SELECT
        a,
        SUM(a) OVER (ORDER BY a ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS O1,
        SUM(a) OVER (ORDER BY a ROWS BETWEEN 2 PRECEDING AND 3 FOLLOWING) AS O2,
        SUM(a) OVER (ORDER BY a ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) AS O3,
        SUM(a) OVER (ORDER BY a ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING) AS O4,
        SUM(a) OVER (ORDER BY a ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS O5,
        SUM(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS O6,
        SUM(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND 3 FOLLOWING) AS O7,
        SUM(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS O8,
        SUM(a) OVER (ORDER BY a ROWS BETWEEN 3 FOLLOWING AND 3 FOLLOWING) AS O9,
        SUM(a) OVER (ORDER BY a ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS O10
    FROM tmp
    """
    )
    expected_df = pd.DataFrame(
        {
            "a": return_df.a,
            "O1": pd.Series([0, 1, 3, 6, 9], dtype="Int64"),
            "O2": pd.Series([6, 10, 10, 10, 9], dtype="Int64"),
            "O3": pd.Series([10, 10, 10, 10, 9], dtype="Int64"),
            "O4": pd.Series([6, 10, 9, 7, 4], dtype="Int64"),
            "O5": pd.Series([10, 10, 9, 7, 4], dtype="Int64"),
            "O6": pd.Series([0, 1, 3, 6, 10], dtype="Int64"),
            "O7": pd.Series([6, 10, 10, 10, 10], dtype="Int64"),
            "O8": pd.Series([10, 10, 10, 10, 10], dtype="Int64"),
            "O9": pd.Series([3, 4, None, None, None], dtype="Int64"),
            "O10": pd.Series([None, 0, 1, 3, 6], dtype="Int64"),
        }
    )

    assert_eq(return_df, expected_df, check_index=False)
