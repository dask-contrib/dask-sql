import dask.dataframe as dd
import pandas as pd
import pytest

from dask_sql.context import Context
from tests.utils import assert_eq


@pytest.mark.parametrize(
    "input_table_1,input_df",
    [
        ("user_table_1", "df"),
        pytest.param("gpu_user_table_1", "gpu_df", marks=pytest.mark.gpu),
    ],
)
def test_sort(c, input_table_1, input_df, request):
    user_table_1 = request.getfixturevalue(input_table_1)
    df = request.getfixturevalue(input_df)

    df_result = c.sql(
        f"""
    SELECT
        *
    FROM {input_table_1}
    ORDER BY b, user_id DESC
    """
    )
    df_expected = user_table_1.sort_values(["b", "user_id"], ascending=[True, False])

    assert_eq(df_result, df_expected, check_index=False)

    df_result = c.sql(
        f"""
    SELECT
        *
    FROM {input_df}
    ORDER BY b DESC, a DESC
    """
    )
    df_expected = df.sort_values(["b", "a"], ascending=[False, False])

    assert_eq(df_result, df_expected, check_index=False)

    df_result = c.sql(
        f"""
    SELECT
        *
    FROM {input_df}
    ORDER BY a DESC, b
    """
    )
    df_expected = df.sort_values(["a", "b"], ascending=[False, True])

    assert_eq(df_result, df_expected, check_index=False)

    df_result = c.sql(
        f"""
    SELECT
        *
    FROM {input_df}
    ORDER BY b, a
    """
    )
    df_expected = df.sort_values(["b", "a"], ascending=[True, True])

    assert_eq(df_result, df_expected, check_index=False)


@pytest.mark.parametrize(
    "input_table_1",
    ["user_table_1", pytest.param("gpu_user_table_1", marks=pytest.mark.gpu)],
)
def test_sort_by_alias(c, input_table_1, request):
    user_table_1 = request.getfixturevalue(input_table_1)

    df_result = c.sql(
        f"""
    SELECT
        b AS my_column
    FROM {input_table_1}
    ORDER BY my_column, user_id DESC
    """
    ).rename(columns={"my_column": "b"})
    df_expected = user_table_1.sort_values(["b", "user_id"], ascending=[True, False])[
        ["b"]
    ]

    assert_eq(df_result, df_expected, check_index=False)


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_sort_with_nan(gpu):
    c = Context()
    df = pd.DataFrame(
        {"a": [1, 2, float("nan"), 2], "b": [4, float("nan"), 5, float("inf")]}
    )
    c.create_table("df", df, gpu=gpu)

    df_result = c.sql("SELECT * FROM df ORDER BY a")
    assert_eq(
        df_result,
        pd.DataFrame(
            {"a": [1, 2, 2, float("nan")], "b": [4, float("nan"), float("inf"), 5]}
        ),
        check_index=False,
    )

    df_result = c.sql("SELECT * FROM df ORDER BY a NULLS FIRST")
    assert_eq(
        df_result,
        pd.DataFrame(
            {"a": [float("nan"), 1, 2, 2], "b": [5, 4, float("nan"), float("inf")]}
        ),
        check_index=False,
    )

    df_result = c.sql("SELECT * FROM df ORDER BY a NULLS LAST")
    assert_eq(
        df_result,
        pd.DataFrame(
            {"a": [1, 2, 2, float("nan")], "b": [4, float("nan"), float("inf"), 5]}
        ),
        check_index=False,
    )

    df_result = c.sql("SELECT * FROM df ORDER BY a ASC")
    assert_eq(
        df_result,
        pd.DataFrame(
            {"a": [1, 2, 2, float("nan")], "b": [4, float("nan"), float("inf"), 5]}
        ),
        check_index=False,
    )

    df_result = c.sql("SELECT * FROM df ORDER BY a ASC NULLS FIRST")
    assert_eq(
        df_result,
        pd.DataFrame(
            {"a": [float("nan"), 1, 2, 2], "b": [5, 4, float("nan"), float("inf")]}
        ),
        check_index=False,
    )

    df_result = c.sql("SELECT * FROM df ORDER BY a ASC NULLS LAST")
    assert_eq(
        df_result,
        pd.DataFrame(
            {"a": [1, 2, 2, float("nan")], "b": [4, float("nan"), float("inf"), 5]}
        ),
        check_index=False,
    )

    df_result = c.sql("SELECT * FROM df ORDER BY a DESC")
    assert_eq(
        df_result,
        pd.DataFrame(
            {"a": [float("nan"), 2, 2, 1], "b": [5, float("nan"), float("inf"), 4]}
        ),
        check_index=False,
    )

    df_result = c.sql("SELECT * FROM df ORDER BY a DESC NULLS FIRST")
    assert_eq(
        df_result,
        pd.DataFrame(
            {"a": [float("nan"), 2, 2, 1], "b": [5, float("nan"), float("inf"), 4]}
        ),
        check_index=False,
    )

    df_result = c.sql("SELECT * FROM df ORDER BY a DESC NULLS LAST")
    assert_eq(
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
    assert_eq(
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
    assert_eq(
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
    assert_eq(
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
    df = pd.DataFrame(
        {
            "a": [float("nan"), 1] * 30,
            "b": [1, 2, 3] * 20,
        }
    )
    c.create_table("df", dd.from_pandas(df, npartitions=10), gpu=gpu)

    df_result = c.sql("SELECT * FROM df ORDER BY a NULLS FIRST, b ASC NULLS FIRST")

    assert_eq(
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

    assert_eq(
        df_result,
        pd.DataFrame(
            {
                "a": [1] * 30 + [float("nan")] * 30,
            }
        ),
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

    assert_eq(df_result, df_expected, check_index=False)


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_sort_not_allowed(c, gpu):
    table_name = "gpu_user_table_1" if gpu else "user_table_1"

    # Wrong column
    with pytest.raises(Exception):
        c.sql(f"SELECT * FROM {table_name} ORDER BY 42")


@pytest.mark.xfail(Reason="Projection step before sort currently failing")
@pytest.mark.parametrize(
    "input_table_1",
    ["user_table_1", pytest.param("gpu_user_table_1", marks=pytest.mark.gpu)],
)
def test_sort_by_old_alias(c, input_table_1, request):
    user_table_1 = request.getfixturevalue(input_table_1)

    df_result = c.sql(
        f"""
    SELECT
        b AS my_column
    FROM {input_table_1}
    ORDER BY b, user_id DESC
    """
    ).rename(columns={"my_column": "b"})
    df_expected = user_table_1.sort_values(["b", "user_id"], ascending=[True, False])[
        ["b"]
    ]

    assert_eq(df_result, df_expected, check_index=False)

    df_result = c.sql(
        f"""
    SELECT
        b*-1 AS my_column
    FROM {input_table_1}
    ORDER BY b, user_id DESC
    """
    ).rename(columns={"my_column": "b"})
    df_expected = user_table_1.sort_values(["b", "user_id"], ascending=[True, False])[
        ["b"]
    ]
    df_expected["b"] *= -1
    assert_eq(df_result, df_expected, check_index=False)

    df_result = c.sql(
        f"""
    SELECT
        b*-1 AS my_column
    FROM {input_table_1}
    ORDER BY my_column, user_id DESC
    """
    ).rename(columns={"my_column": "b"})
    df_expected["b"] *= -1
    df_expected = user_table_1.sort_values(["b", "user_id"], ascending=[True, False])[
        ["b"]
    ]
