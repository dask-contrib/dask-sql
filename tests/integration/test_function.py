import numpy as np
from pandas.testing import assert_frame_equal
import dask.dataframe as dd
import pytest


def test_custom_function(c, df):
    def f(x):
        return x ** 2

    c.register_function(f, "f", [("x", np.float64)], np.float64)

    return_df = c.sql(
        """
        SELECT f(a) AS a
        FROM df
        """
    )
    return_df = return_df.compute()

    assert_frame_equal(return_df.reset_index(drop=True), df[["a"]] ** 2)


def test_multiple_definitions(c, df_simple):
    def f(x):
        return x ** 2

    c.register_function(f, "f", [("x", np.float64)], np.float64)
    c.register_function(f, "f", [("x", np.int64)], np.int64)

    return_df = c.sql(
        """
        SELECT f(a) AS a, f(b) AS b
        FROM df_simple
        """
    )
    return_df = return_df.compute()

    assert_frame_equal(return_df.reset_index(drop=True), df_simple[["a", "b"]] ** 2)


def test_aggregate_function(c):
    fagg = dd.Aggregation("f", lambda x: x.sum(), lambda x: x.sum())
    c.register_aggregation(fagg, "fagg", [("x", np.float64)], np.float64)

    return_df = c.sql(
        """
        SELECT fagg(b) AS test, sum(b) AS "S"
        FROM df
        """
    )
    return_df = return_df.compute()

    assert (return_df["test"] == return_df["S"]).all()


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

    fagg = dd.Aggregation("f", lambda x: x.sum(), lambda x: x.sum())
    c.register_aggregation(fagg, "fagg", [("x", np.float64)], np.float64)
    c.register_aggregation(fagg, "fagg", [("x", np.int64)], np.int64)

    fagg = dd.Aggregation("f", lambda x: x.mean(), lambda x: x.mean())

    with pytest.raises(ValueError):
        c.register_aggregation(fagg, "fagg", [("x", np.float64)], np.float64)
