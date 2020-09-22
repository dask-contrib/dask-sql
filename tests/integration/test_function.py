import numpy as np
from pandas.testing import assert_frame_equal
import dask.dataframe as dd

from tests.integration.fixtures import DaskTestCase


class FunctionTestCase(DaskTestCase):
    def test_custom_function(self):
        def f(x):
            return x ** 2

        self.c.register_function(f, "f", [("x", np.float64)], np.float64)

        df = self.c.sql(
            """
            SELECT f(a) AS a
            FROM df
            """
        )
        df = df.compute()

        assert_frame_equal(df.reset_index(drop=True), self.df[["a"]] ** 2)

    def test_aggregate_function(self):
        fagg = dd.Aggregation("f", lambda x: x.sum(), lambda x: x.sum())
        self.c.register_aggregation(fagg, "fagg", [("x", np.float64)], np.float64)

        df = self.c.sql(
            """
            SELECT fagg(b) AS test, sum(b) AS "S"
            FROM df
            """
        )
        df = df.compute()

        assert (df["test"] == df["S"]).all()
