import dask.dataframe as dd
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

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
    df_result = df_result.compute()
    df_expected = user_table_1.sort_values(["b", "user_id"], ascending=[True, False])

    assert_frame_equal(
        df_result.reset_index(drop=True), df_expected.reset_index(drop=True)
    )

    df_result = c.sql(
        """
    SELECT
        *
    FROM df
    ORDER BY b DESC, a DESC
    """
    )
    df_result = df_result.compute()
    df_expected = df.sort_values(["b", "a"], ascending=[False, False])

    assert_frame_equal(
        df_result.reset_index(drop=True), df_expected.reset_index(drop=True)
    )

    df_result = c.sql(
        """
    SELECT
        *
    FROM df
    ORDER BY a DESC, b
    """
    )
    df_result = df_result.compute()
    df_expected = df.sort_values(["a", "b"], ascending=[False, True])

    assert_frame_equal(
        df_result.reset_index(drop=True), df_expected.reset_index(drop=True)
    )

    df_result = c.sql(
        """
    SELECT
        *
    FROM df
    ORDER BY b, a
    """
    )
    df_result = df_result.compute()
    df_expected = df.sort_values(["b", "a"], ascending=[True, True])

    assert_frame_equal(
        df_result.reset_index(drop=True), df_expected.reset_index(drop=True)
    )


def test_sort_by_alias(c, user_table_1):
    df_result = c.sql(
        """
    SELECT
        b AS my_column
    FROM user_table_1
    ORDER BY my_column, user_id DESC
    """
    )
    df_result = (
        df_result.compute().reset_index(drop=True).rename(columns={"my_column": "b"})
    )
    df_expected = user_table_1.sort_values(
        ["b", "user_id"], ascending=[True, False]
    ).reset_index(drop=True)[["b"]]

    assert_frame_equal(df_result, df_expected)


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_sort_with_nan(gpu):
    if gpu:
        xd = pytest.importorskip("cudf")
    else:
        xd = pd

    c = Context()
    df = xd.DataFrame(
        {"a": [1, 2, float("nan"), 2], "b": [4, float("nan"), 5, float("inf")]}
    )
    c.create_table("df", df)

    df_result = c.sql("SELECT * FROM df ORDER BY a").compute().reset_index(drop=True)
    dd.assert_eq(
        df_result,
        xd.DataFrame(
            {"a": [1, 2, 2, float("nan")], "b": [4, float("nan"), float("inf"), 5]}
        ),
    )

    df_result = (
        c.sql("SELECT * FROM df ORDER BY a NULLS FIRST")
        .compute()
        .reset_index(drop=True)
    )
    dd.assert_eq(
        df_result,
        xd.DataFrame(
            {"a": [float("nan"), 1, 2, 2], "b": [5, 4, float("nan"), float("inf")]}
        ),
    )

    df_result = (
        c.sql("SELECT * FROM df ORDER BY a NULLS LAST").compute().reset_index(drop=True)
    )
    dd.assert_eq(
        df_result,
        xd.DataFrame(
            {"a": [1, 2, 2, float("nan")], "b": [4, float("nan"), float("inf"), 5]}
        ),
    )

    df_result = (
        c.sql("SELECT * FROM df ORDER BY a ASC").compute().reset_index(drop=True)
    )
    dd.assert_eq(
        df_result,
        xd.DataFrame(
            {"a": [1, 2, 2, float("nan")], "b": [4, float("nan"), float("inf"), 5]}
        ),
    )

    df_result = (
        c.sql("SELECT * FROM df ORDER BY a ASC NULLS FIRST")
        .compute()
        .reset_index(drop=True)
    )
    dd.assert_eq(
        df_result,
        xd.DataFrame(
            {"a": [float("nan"), 1, 2, 2], "b": [5, 4, float("nan"), float("inf")]}
        ),
    )

    df_result = (
        c.sql("SELECT * FROM df ORDER BY a ASC NULLS LAST")
        .compute()
        .reset_index(drop=True)
    )
    dd.assert_eq(
        df_result,
        xd.DataFrame(
            {"a": [1, 2, 2, float("nan")], "b": [4, float("nan"), float("inf"), 5]}
        ),
    )

    df_result = (
        c.sql("SELECT * FROM df ORDER BY a DESC").compute().reset_index(drop=True)
    )
    dd.assert_eq(
        df_result,
        xd.DataFrame(
            {"a": [float("nan"), 2, 2, 1], "b": [5, float("nan"), float("inf"), 4]}
        ),
    )

    df_result = (
        c.sql("SELECT * FROM df ORDER BY a DESC NULLS FIRST")
        .compute()
        .reset_index(drop=True)
    )
    dd.assert_eq(
        df_result,
        xd.DataFrame(
            {"a": [float("nan"), 2, 2, 1], "b": [5, float("nan"), float("inf"), 4]}
        ),
    )

    df_result = (
        c.sql("SELECT * FROM df ORDER BY a DESC NULLS LAST")
        .compute()
        .reset_index(drop=True)
    )
    dd.assert_eq(
        df_result,
        xd.DataFrame(
            {"a": [2, 2, 1, float("nan")], "b": [float("nan"), float("inf"), 4, 5]}
        ),
    )


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_sort_with_nan_more_columns(gpu):
    if gpu:
        xd = pytest.importorskip("cudf")
    else:
        xd = pd

    c = Context()
    df = xd.DataFrame(
        {
            "a": [1, 1, 2, 2, float("nan"), float("nan")],
            "b": [1, 1, 2, float("nan"), float("inf"), 5],
            "c": [1, float("nan"), 3, 4, 5, 6],
        }
    )
    c.create_table("df", df)

    df_result = c.sql(
        "SELECT * FROM df ORDER BY a ASC NULLS FIRST, b DESC NULLS LAST, c ASC NULLS FIRST"
    )
    dd.assert_eq(
        df_result,
        xd.DataFrame(
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
        xd.DataFrame(
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
        xd.DataFrame(
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
    if gpu:
        xd = pytest.importorskip("cudf")
    else:
        xd = pd

    c = Context()
    df = xd.DataFrame({"a": [float("nan"), 1] * 30, "b": [1, 2, 3] * 20,})
    c.create_table("df", dd.from_pandas(df, npartitions=10))

    df_result = (
        c.sql("SELECT * FROM df ORDER BY a NULLS FIRST, b ASC NULLS FIRST")
        .compute()
        .reset_index(drop=True)
    )

    dd.assert_eq(
        df_result,
        xd.DataFrame(
            {
                "a": [float("nan")] * 30 + [1] * 30,
                "b": [1] * 10 + [2] * 10 + [3] * 10 + [1] * 10 + [2] * 10 + [3] * 10,
            }
        ),
    )

    df = xd.DataFrame({"a": [float("nan"), 1] * 30})
    c.create_table("df", dd.from_pandas(df, npartitions=10))

    df_result = c.sql("SELECT * FROM df ORDER BY a").compute().reset_index(drop=True)

    dd.assert_eq(df_result, xd.DataFrame({"a": [1] * 30 + [float("nan")] * 30,}))


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_sort_strings(c, gpu):
    if gpu:
        xd = pytest.importorskip("cudf")
    else:
        xd = pd

    string_table = xd.DataFrame({"a": ["zzhsd", "öfjdf", "baba"]})
    c.create_table("string_table", string_table)

    df_result = c.sql(
        """
    SELECT
        *
    FROM string_table
    ORDER BY a
    """
    )
    df_result = df_result.compute().reset_index(drop=True)
    df_expected = string_table.sort_values(["a"], ascending=True).reset_index(drop=True)

    dd.assert_eq(df_result, df_expected)


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_sort_not_allowed(c, gpu):
    if gpu:
        table_name = "gpu_user_table_1"
    else:
        table_name = "user_table_1"

    # Wrong column
    with pytest.raises(Exception):
        c.sql(f"SELECT * FROM {table_name} ORDER BY 42")


def test_limit(c, long_table):
    df_result = c.sql("SELECT * FROM long_table LIMIT 101")
    df_result = df_result.compute()

    assert_frame_equal(df_result, long_table.iloc[:101])

    df_result = c.sql("SELECT * FROM long_table LIMIT 200")
    df_result = df_result.compute()

    assert_frame_equal(df_result, long_table.iloc[:200])

    df_result = c.sql("SELECT * FROM long_table LIMIT 100")
    df_result = df_result.compute()

    assert_frame_equal(df_result, long_table.iloc[:100])

    df_result = c.sql("SELECT * FROM long_table LIMIT 100 OFFSET 99")
    df_result = df_result.compute()

    assert_frame_equal(df_result, long_table.iloc[99 : 99 + 100])

    df_result = c.sql("SELECT * FROM long_table LIMIT 100 OFFSET 100")
    df_result = df_result.compute()

    assert_frame_equal(df_result, long_table.iloc[100 : 100 + 100])

    df_result = c.sql("SELECT * FROM long_table LIMIT 101 OFFSET 101")
    df_result = df_result.compute()

    assert_frame_equal(df_result, long_table.iloc[101 : 101 + 101])

    df_result = c.sql("SELECT * FROM long_table OFFSET 101")
    df_result = df_result.compute()

    assert_frame_equal(df_result, long_table.iloc[101:])


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
    df_result = df_result.compute()
    df_expected = gpu_user_table_1.sort_values(
        ["b", "user_id"], ascending=[True, False]
    )

    dd.assert_eq(df_result.reset_index(drop=True), df_expected.reset_index(drop=True))

    df_result = c.sql(
        """
    SELECT
        *
    FROM gpu_df
    ORDER BY b DESC, a DESC
    """
    )
    df_result = df_result.compute()
    df_expected = gpu_df.sort_values(["b", "a"], ascending=[False, False])

    dd.assert_eq(df_result.reset_index(drop=True), df_expected.reset_index(drop=True))

    df_result = c.sql(
        """
    SELECT
        *
    FROM gpu_df
    ORDER BY a DESC, b
    """
    )
    df_result = df_result.compute()
    df_expected = gpu_df.sort_values(["a", "b"], ascending=[False, True])

    dd.assert_eq(df_result.reset_index(drop=True), df_expected.reset_index(drop=True))

    df_result = c.sql(
        """
    SELECT
        *
    FROM gpu_df
    ORDER BY b, a
    """
    )
    df_result = df_result.compute()
    df_expected = gpu_df.sort_values(["b", "a"], ascending=[True, True])

    dd.assert_eq(df_result.reset_index(drop=True), df_expected.reset_index(drop=True))


@pytest.mark.gpu
def test_sort_gpu_by_alias(c, gpu_user_table_1):
    df_result = c.sql(
        """
    SELECT
        b AS my_column
    FROM gpu_user_table_1
    ORDER BY my_column, user_id DESC
    """
    )
    df_result = (
        df_result.compute().reset_index(drop=True).rename(columns={"my_column": "b"})
    )
    df_expected = gpu_user_table_1.sort_values(
        ["b", "user_id"], ascending=[True, False]
    ).reset_index(drop=True)[["b"]]

    dd.assert_eq(df_result, df_expected)


@pytest.mark.gpu
def test_limit_gpu(c, gpu_long_table):
    df_result = c.sql("SELECT * FROM gpu_long_table LIMIT 101")
    df_result = df_result.compute()

    dd.assert_eq(df_result, gpu_long_table.iloc[:101])

    df_result = c.sql("SELECT * FROM gpu_long_table LIMIT 200")
    df_result = df_result.compute()

    dd.assert_eq(df_result, gpu_long_table.iloc[:200])

    df_result = c.sql("SELECT * FROM gpu_long_table LIMIT 100")
    df_result = df_result.compute()

    dd.assert_eq(df_result, gpu_long_table.iloc[:100])

    df_result = c.sql("SELECT * FROM gpu_long_table LIMIT 100 OFFSET 99")
    df_result = df_result.compute()

    dd.assert_eq(df_result, gpu_long_table.iloc[99 : 99 + 100])

    df_result = c.sql("SELECT * FROM gpu_long_table LIMIT 100 OFFSET 100")
    df_result = df_result.compute()

    dd.assert_eq(df_result, gpu_long_table.iloc[100 : 100 + 100])

    df_result = c.sql("SELECT * FROM gpu_long_table LIMIT 101 OFFSET 101")
    df_result = df_result.compute()

    dd.assert_eq(df_result, gpu_long_table.iloc[101 : 101 + 101])
