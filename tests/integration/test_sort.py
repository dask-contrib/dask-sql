import dask.dataframe as dd
import pandas as pd
import pytest

from dask_sql.context import Context
from tests.utils import assert_eq


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

    assert_eq(df_result, df_expected, check_index=False)

    df_result = c.sql(
        """
    SELECT
        *
    FROM df
    ORDER BY b DESC, a DESC
    """
    )
    df_expected = df.sort_values(["b", "a"], ascending=[False, False])

    assert_eq(df_result, df_expected, check_index=False)

    df_result = c.sql(
        """
    SELECT
        *
    FROM df
    ORDER BY a DESC, b
    """
    )
    df_expected = df.sort_values(["a", "b"], ascending=[False, True])

    assert_eq(df_result, df_expected, check_index=False)

    df_result = c.sql(
        """
    SELECT
        *
    FROM df
    ORDER BY b, a
    """
    )
    df_expected = df.sort_values(["b", "a"], ascending=[True, True])

    assert_eq(df_result, df_expected, check_index=False)


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

    assert_eq(df_result, df_expected, check_index=False)


def test_sort_with_nan(c):
    df = pd.DataFrame(
        {"a": [1, 2, float("nan"), 2], "b": [4, float("nan"), 5, float("inf")]}
    )
    c.create_table("df", df)

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


def test_sort_with_nan_more_columns(c):
    df = pd.DataFrame(
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


def test_sort_with_nan_many_partitions(c):
    c = Context()
    df = pd.DataFrame(
        {
            "a": [float("nan"), 1] * 30,
            "b": [1, 2, 3] * 20,
        }
    )
    c.create_table("df", dd.from_pandas(df, npartitions=10))

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


def test_sort_strings(c, string_table):
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


def test_sort_not_allowed(c):
    # Wrong column
    with pytest.raises(Exception):
        c.sql("SELECT * FROM user_table_1 ORDER BY 42")


def test_sort_by_old_alias(c, user_table_1):
    df_result = c.sql(
        """
    SELECT
        b AS my_column
    FROM user_table_1
    ORDER BY b, user_id DESC
    """
    ).rename(columns={"my_column": "b"})
    df_expected = user_table_1.sort_values(["b", "user_id"], ascending=[True, False])[
        ["b"]
    ]

    assert_eq(df_result, df_expected, check_index=False)

    df_result = c.sql(
        """
    SELECT
        b*-1 AS my_column
    FROM user_table_1
    ORDER BY b, user_id DESC
    """
    ).rename(columns={"my_column": "b"})
    df_expected = user_table_1.sort_values(["b", "user_id"], ascending=[True, False])[
        ["b"]
    ]
    df_expected["b"] *= -1
    assert_eq(df_result, df_expected, check_index=False)

    df_result = c.sql(
        """
    SELECT
        b*-1 AS my_column
    FROM user_table_1
    ORDER BY my_column, user_id DESC
    """
    ).rename(columns={"my_column": "b"})
    df_expected["b"] *= -1
    df_expected = user_table_1.sort_values(["b", "user_id"], ascending=[True, False])[
        ["b"]
    ]


def test_sort_topk(c):
    df = pd.DataFrame(
        {
            "a": [float("nan"), 1] * 30,
            "b": [1, 2, 3] * 20,
            "c": ["a", "b", "c"] * 20,
        }
    )
    c.create_table("df", dd.from_pandas(df, npartitions=10))

    df_result = c.sql("""SELECT * FROM df ORDER BY a LIMIT 10""")
    assert any(["nsmallest" in key for key in df_result.dask.layers.keys()])
    assert_eq(
        df_result,
        pd.DataFrame(
            {
                "a": [1.0] * 10,
                "b": ([2, 1, 3] * 4)[:10],
                "c": (["b", "a", "c"] * 4)[:10],
            }
        ),
        check_index=False,
    )

    df_result = c.sql("""SELECT * FROM df ORDER BY a, b LIMIT 10""")
    assert any(["nsmallest" in key for key in df_result.dask.layers.keys()])
    assert_eq(
        df_result,
        pd.DataFrame({"a": [1.0] * 10, "b": [1] * 10, "c": ["a"] * 10}),
        check_index=False,
    )

    df_result = c.sql(
        """SELECT * FROM df ORDER BY a DESC NULLS LAST, b DESC NULLS LAST LIMIT 10"""
    )
    assert any(["nlargest" in key for key in df_result.dask.layers.keys()])
    assert_eq(
        df_result,
        pd.DataFrame({"a": [1.0] * 10, "b": [3] * 10, "c": ["c"] * 10}),
        check_index=False,
    )

    # String column nlargest/smallest not supported for pandas
    df_result = c.sql("""SELECT * FROM df ORDER BY c LIMIT 10""")
    if not c.gpu:
        assert all(["nlargest" not in key for key in df_result.dask.layers.keys()])
        assert all(["nsmallest" not in key for key in df_result.dask.layers.keys()])
    else:
        assert_eq(
            df_result,
            pd.DataFrame({"a": [float("nan"), 1] * 5, "b": [1] * 10, "c": ["a"] * 10}),
            check_index=False,
        )

    # Assert that the optimization isn't applied when there is any nulls first
    df_result = c.sql(
        """SELECT * FROM df ORDER BY a DESC, b DESC NULLS LAST LIMIT 10"""
    )
    assert all(["nlargest" not in key for key in df_result.dask.layers.keys()])
    assert all(["nsmallest" not in key for key in df_result.dask.layers.keys()])

    # Assert optimization isn't applied for mixed asc + desc sort
    df_result = c.sql("""SELECT * FROM df ORDER BY a, b DESC NULLS LAST LIMIT 10""")
    assert all(["nlargest" not in key for key in df_result.dask.layers.keys()])
    assert all(["nsmallest" not in key for key in df_result.dask.layers.keys()])

    # Assert optimization isn't applied when the number of requested elements
    # exceed topk-nelem-limit config value
    # Default topk-nelem-limit is 1M and 334k*3columns takes it above this limit
    df_result = c.sql("""SELECT * FROM df ORDER BY a, b LIMIT 333334""")
    assert all(["nlargest" not in key for key in df_result.dask.layers.keys()])
    assert all(["nsmallest" not in key for key in df_result.dask.layers.keys()])

    df_result = c.sql(
        """SELECT * FROM df ORDER BY a, b LIMIT 10""",
        config_options={"sql.sort.topk-nelem-limit": 29},
    )
    assert all(["nlargest" not in key for key in df_result.dask.layers.keys()])
    assert all(["nsmallest" not in key for key in df_result.dask.layers.keys()])
