import itertools
import operator

import dask.dataframe as dd
import numpy as np
import pytest

from tests.utils import assert_eq


def test_custom_function(c, df):
    def f(x):
        return x ** 2

    c.register_function(f, "f", [("x", np.float64)], np.float64)

    return_df = c.sql("SELECT F(a) AS a FROM df")

    assert_eq(return_df, df[["a"]] ** 2)


def test_custom_function_row(c, df):
    def f(row):
        return row["x"] ** 2

    c.register_function(f, "f", [("x", np.float64)], np.float64, row_udf=True)

    return_df = c.sql("SELECT F(a) AS a FROM df")

    assert_eq(return_df, df[["a"]] ** 2)


@pytest.mark.parametrize("colnames", list(itertools.combinations(["a", "b", "c"], 2)))
def test_custom_function_any_colnames(colnames, df_wide, c):
    # a third column is needed

    def f(row):
        return row["x"] + row["y"]

    colname_x, colname_y = colnames
    c.register_function(
        f, "f", [("x", np.int64), ("y", np.int64)], np.int64, row_udf=True
    )

    return_df = c.sql(f"SELECT F({colname_x},{colname_y}) FROM df_wide")

    expect = df_wide[colname_x] + df_wide[colname_y]
    got = return_df.iloc[:, 0]

    assert_eq(expect, got, check_names=False)


@pytest.mark.parametrize(
    "retty",
    [None, np.float64, np.float32, np.int64, np.int32, np.int16, np.int8, np.bool_],
)
def test_custom_function_row_return_types(c, df, retty):
    def f(row):
        return row["x"] ** 2

    if retty is None:
        with pytest.raises(ValueError):
            c.register_function(f, "f", [("x", np.float64)], retty, row_udf=True)
        return

    c.register_function(f, "f", [("x", np.float64)], retty, row_udf=True)

    return_df = c.sql("SELECT F(a) AS a FROM df")

    assert_eq(return_df, (df[["a"]] ** 2).astype(retty))


# Test row UDFs with one arg
@pytest.mark.parametrize("k", [1, 1.5, True])
@pytest.mark.parametrize(
    "op", [operator.add, operator.sub, operator.mul, operator.truediv]
)
@pytest.mark.parametrize("retty", [np.int64, np.float64, np.bool_])
def test_custom_function_row_args(c, df, k, op, retty):
    const_type = np.dtype(type(k)).type

    def f(row, k):
        return op(row["a"], k)

    c.register_function(
        f, "f", [("a", np.int64), ("k", const_type)], retty, row_udf=True
    )

    return_df = c.sql(f"SELECT F(a, {k}) as a from df")
    expected_df = op(df[["a"]], k).astype(retty)

    assert_eq(return_df, expected_df)


# Test row UDFs with two args
@pytest.mark.parametrize("k2", [1, 1.5, True])
@pytest.mark.parametrize("k1", [1, 1.5, True])
@pytest.mark.parametrize(
    "op", [operator.add, operator.sub, operator.mul, operator.truediv]
)
@pytest.mark.parametrize("retty", [np.int64, np.float64, np.bool_])
def test_custom_function_row_two_args(c, df, k1, k2, op, retty):
    const_type_k1 = np.dtype(type(k1)).type
    const_type_k2 = np.dtype(type(k2)).type

    def f(row, k1, k2):
        x = op(row["a"], k1)
        y = op(x, k2)

        return y

    c.register_function(
        f,
        "f",
        [("a", np.int64), ("k1", const_type_k1), ("k2", const_type_k2)],
        retty,
        row_udf=True,
    )

    return_df = c.sql(f"SELECT F(a, {k1}, {k2}) as a from df")
    expected_df = op(op(df[["a"]], k1), k2).astype(retty)

    assert_eq(return_df, expected_df)


def test_multiple_definitions(c, df_simple):
    def f(x):
        return x ** 2

    c.register_function(f, "f", [("x", np.float64)], np.float64)
    c.register_function(f, "f", [("x", np.int64)], np.int64)

    return_df = c.sql(
        """
        SELECT F(a) AS a, f(b) AS b
        FROM df_simple
        """
    )
    expected_df = df_simple[["a", "b"]] ** 2

    assert_eq(return_df, expected_df)

    def f(x):
        return x ** 3

    c.register_function(f, "f", [("x", np.float64)], np.float64, replace=True)
    c.register_function(f, "f", [("x", np.int64)], np.int64)

    return_df = c.sql(
        """
        SELECT F(a) AS a, f(b) AS b
        FROM df_simple
        """
    )
    expected_df = df_simple[["a", "b"]] ** 3

    assert_eq(return_df, expected_df)


def test_aggregate_function(c):
    fagg = dd.Aggregation("f", lambda x: x.sum(), lambda x: x.sum())
    c.register_aggregation(fagg, "fagg", [("x", np.float64)], np.float64)

    return_df = c.sql(
        """
        SELECT FAGG(b) AS test, SUM(b) AS "S"
        FROM df
        """
    )

    assert_eq(return_df["test"], return_df["S"], check_names=False)


def test_reregistration(c):
    def f(x):
        return x ** 2

    # The same is fine
    c.register_function(f, "f", [("x", np.float64)], np.float64)
    c.register_function(f, "f", [("x", np.int64)], np.int64)

    def f(x):
        return x ** 3

    # A different not
    with pytest.raises(ValueError):
        c.register_function(f, "f", [("x", np.float64)], np.float64)

    # only if we replace it
    c.register_function(f, "f", [("x", np.float64)], np.float64, replace=True)

    fagg = dd.Aggregation("f", lambda x: x.sum(), lambda x: x.sum())
    c.register_aggregation(fagg, "fagg", [("x", np.float64)], np.float64)
    c.register_aggregation(fagg, "fagg", [("x", np.int64)], np.int64)

    fagg = dd.Aggregation("f", lambda x: x.mean(), lambda x: x.mean())

    with pytest.raises(ValueError):
        c.register_aggregation(fagg, "fagg", [("x", np.float64)], np.float64)

    c.register_aggregation(fagg, "fagg", [("x", np.float64)], np.float64, replace=True)
