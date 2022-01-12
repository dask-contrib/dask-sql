import dask.dataframe as dd
import pandas as pd
import pytest

from dask_sql.context import Context


def test_sort(c, user_table_1, df):
    df_result = c.sql(
        """
    SELECT
        *
    FROM user_table_1
    ORDER BY b, user_id DESC
    """
    )
    df_expected = user_table_1.sort_values(["b", "user_id"], ascending=[True, False])

    dd.assert_eq(df_result, df_expected, check_index=False)

    df_result = c.sql(
        """
    SELECT
        *
    FROM df
    ORDER BY b DESC, a DESC
    """
    )
    df_expected = df.sort_values(["b", "a"], ascending=[False, False])

    dd.assert_eq(df_result, df_expected, check_index=False)

    df_result = c.sql(
        """
    SELECT
        *
    FROM df
    ORDER BY a DESC, b
    """
    )
    df_expected = df.sort_values(["a", "b"], ascending=[False, True])

    dd.assert_eq(df_result, df_expected, check_index=False)

    df_result = c.sql(
        """
    SELECT
        *
    FROM df
    ORDER BY b, a
    """
    )
    df_expected = df.sort_values(["b", "a"], ascending=[True, True])

    dd.assert_eq(df_result, df_expected, check_index=False)


def test_sort_by_alias(c, user_table_1):
    df_result = c.sql(
        """
    SELECT
        b AS my_column
    FROM user_table_1
    ORDER BY my_column, user_id DESC
    """
    ).rename(columns={"my_column": "b"})
    df_expected = user_table_1.sort_values(["b", "user_id"], ascending=[True, False])[
        ["b"]
    ]

    dd.assert_eq(df_result, df_expected, check_index=False)


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_sort_with_nan(gpu):
    c = Context()
    df = pd.DataFrame(
        {"a": [1, 2, float("nan"), 2], "b": [4, float("nan"), 5, float("inf")]}
    )
    c.create_table("df", df, gpu=gpu)

    df_result = c.sql("SELECT * FROM df ORDER BY a")
    dd.assert_eq(
        df_result,
        pd.DataFrame(
            {"a": [1, 2, 2, float("nan")], "b": [4, float("nan"), float("inf"), 5]}
        ),
        check_index=False,
    )

    df_result = c.sql("SELECT * FROM df ORDER BY a NULLS FIRST")
    dd.assert_eq(
        df_result,
        pd.DataFrame(
            {"a": [float("nan"), 1, 2, 2], "b": [5, 4, float("nan"), float("inf")]}
        ),
        check_index=False,
    )

    df_result = c.sql("SELECT * FROM df ORDER BY a NULLS LAST")
    dd.assert_eq(
        df_result,
        pd.DataFrame(
            {"a": [1, 2, 2, float("nan")], "b": [4, float("nan"), float("inf"), 5]}
        ),
        check_index=False,
    )

    df_result = c.sql("SELECT * FROM df ORDER BY a ASC")
    dd.assert_eq(
        df_result,
        pd.DataFrame(
            {"a": [1, 2, 2, float("nan")], "b": [4, float("nan"), float("inf"), 5]}
        ),
        check_index=False,
    )

    df_result = c.sql("SELECT * FROM df ORDER BY a ASC NULLS FIRST")
    dd.assert_eq(
        df_result,
        pd.DataFrame(
            {"a": [float("nan"), 1, 2, 2], "b": [5, 4, float("nan"), float("inf")]}
        ),
        check_index=False,
    )

    df_result = c.sql("SELECT * FROM df ORDER BY a ASC NULLS LAST")
    dd.assert_eq(
        df_result,
        pd.DataFrame(
            {"a": [1, 2, 2, float("nan")], "b": [4, float("nan"), float("inf"), 5]}
        ),
        check_index=False,
    )

    df_result = c.sql("SELECT * FROM df ORDER BY a DESC")
    dd.assert_eq(
        df_result,
        pd.DataFrame(
            {"a": [float("nan"), 2, 2, 1], "b": [5, float("nan"), float("inf"), 4]}
        ),
        check_index=False,
    )

    df_result = c.sql("SELECT * FROM df ORDER BY a DESC NULLS FIRST")
    dd.assert_eq(
        df_result,
        pd.DataFrame(
            {"a": [float("nan"), 2, 2, 1], "b": [5, float("nan"), float("inf"), 4]}
        ),
        check_index=False,
    )

    df_result = c.sql("SELECT * FROM df ORDER BY a DESC NULLS LAST")
    dd.assert_eq(
        df_result,
        pd.DataFrame(
            {"a": [2, 2, 1, float("nan")], "b": [float("nan"), float("inf"), 4, 5]}
        ),
        check_index=False,
    )


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_sort_with_nan_more_columns(gpu):
    c = Context()
    df = pd.DataFrame(
        {
            "a": [1, 1, 2, 2, float("nan"), float("nan")],
            "b": [1, 1, 2, float("nan"), float("inf"), 5],
            "c": [1, float("nan"), 3, 4, 5, 6],
        }
    )
    c.create_table("df", df, gpu=gpu)

    df_result = c.sql(
        "SELECT * FROM df ORDER BY a ASC NULLS FIRST, b DESC NULLS LAST, c ASC NULLS FIRST"
    )
    dd.assert_eq(
        df_result,
        pd.DataFrame(
            {
                "a": [float("nan"), float("nan"), 1, 1, 2, 2],
                "b": [float("inf"), 5, 1, 1, 2, float("nan")],
                "c": [5, 6, float("nan"), 1, 3, 4],
            }
        ),
        check_index=False,
    )

    df_result = c.sql(
        "SELECT * FROM df ORDER BY a ASC NULLS LAST, b DESC NULLS FIRST, c DESC NULLS LAST"
    )
    dd.assert_eq(
        df_result,
        pd.DataFrame(
            {
                "a": [1, 1, 2, 2, float("nan"), float("nan")],
                "b": [1, 1, float("nan"), 2, float("inf"), 5],
                "c": [1, float("nan"), 4, 3, 5, 6],
            }
        ),
        check_index=False,
    )

    df_result = c.sql(
        "SELECT * FROM df ORDER BY a ASC NULLS FIRST, b DESC NULLS LAST, c DESC NULLS LAST"
    )
    dd.assert_eq(
        df_result,
        pd.DataFrame(
            {
                "a": [float("nan"), float("nan"), 1, 1, 2, 2],
                "b": [float("inf"), 5, 1, 1, 2, float("nan")],
                "c": [5, 6, 1, float("nan"), 3, 4],
            }
        ),
        check_index=False,
    )


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_sort_with_nan_many_partitions(gpu):
    c = Context()
    df = pd.DataFrame({"a": [float("nan"), 1] * 30, "b": [1, 2, 3] * 20,})
    c.create_table("df", dd.from_pandas(df, npartitions=10), gpu=gpu)

    df_result = c.sql("SELECT * FROM df ORDER BY a NULLS FIRST, b ASC NULLS FIRST")

    dd.assert_eq(
        df_result,
        pd.DataFrame(
            {
                "a": [float("nan")] * 30 + [1] * 30,
                "b": [1] * 10 + [2] * 10 + [3] * 10 + [1] * 10 + [2] * 10 + [3] * 10,
            }
        ),
        check_index=False,
    )

    df = pd.DataFrame({"a": [float("nan"), 1] * 30})
    c.create_table("df", dd.from_pandas(df, npartitions=10))

    df_result = c.sql("SELECT * FROM df ORDER BY a")

    dd.assert_eq(
        df_result,
        pd.DataFrame({"a": [1] * 30 + [float("nan")] * 30,}),
        check_index=False,
    )


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_sort_strings(c, gpu):
    string_table = pd.DataFrame({"a": ["zzhsd", "öfjdf", "baba"]})
    c.create_table("string_table", string_table, gpu=gpu)

    df_result = c.sql(
        """
    SELECT
        *
    FROM string_table
    ORDER BY a
    """
    )

    df_expected = string_table.sort_values(["a"], ascending=True)

    dd.assert_eq(df_result, df_expected, check_index=False)


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_sort_not_allowed(c, gpu):
    table_name = "gpu_user_table_1" if gpu else "user_table_1"

    # Wrong column
    with pytest.raises(Exception):
        c.sql(f"SELECT * FROM {table_name} ORDER BY 42")


@pytest.mark.gpu
def test_sort_gpu(c, gpu_user_table_1, gpu_df):
    df_result = c.sql(
        """
    SELECT
        *
    FROM gpu_user_table_1
    ORDER BY b, user_id DESC
    """
    )

    df_expected = gpu_user_table_1.sort_values(
        ["b", "user_id"], ascending=[True, False]
    )

    dd.assert_eq(df_result, df_expected, check_index=False)

    df_result = c.sql(
        """
    SELECT
        *
    FROM gpu_df
    ORDER BY b DESC, a DESC
    """
    )

    df_expected = gpu_df.sort_values(["b", "a"], ascending=[False, False])

    dd.assert_eq(df_result, df_expected, check_index=False)

    df_result = c.sql(
        """
    SELECT
        *
    FROM gpu_df
    ORDER BY a DESC, b
    """
    )

    df_expected = gpu_df.sort_values(["a", "b"], ascending=[False, True])

    dd.assert_eq(df_result, df_expected, check_index=False)

    df_result = c.sql(
        """
    SELECT
        *
    FROM gpu_df
    ORDER BY b, a
    """
    )

    df_expected = gpu_df.sort_values(["b", "a"], ascending=[True, True])

    dd.assert_eq(df_result, df_expected, check_index=False)


@pytest.mark.gpu
def test_sort_gpu_by_alias(c, gpu_user_table_1):
    df_result = c.sql(
        """
    SELECT
        b AS my_column
    FROM gpu_user_table_1
    ORDER BY my_column, user_id DESC
    """
    ).rename(columns={"my_column": "b"})
    df_expected = gpu_user_table_1.sort_values(
        ["b", "user_id"], ascending=[True, False]
    )[["b"]]

    dd.assert_eq(df_result, df_expected, check_index=False)
