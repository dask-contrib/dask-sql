import pandas as pd
from pandas.testing import assert_frame_equal

from tests.integration.fixtures import DaskTestCase


class SelectTestCase(DaskTestCase):
    def test_select(self):
        df = self.c.sql("SELECT * FROM df")
        df = df.compute()

        assert_frame_equal(df, self.df)

    def test_select_column(self):
        df = self.c.sql("SELECT a FROM df")
        df = df.compute()

        assert_frame_equal(df, self.df[["a"]])

    def test_select_expr(self):
        df = self.c.sql("SELECT a + 1 AS a, b AS bla FROM df")
        df = df.compute()

        expected_df = pd.DataFrame({"a": self.df["a"] + 1, "bla": self.df["b"]})
        assert_frame_equal(df, expected_df)

    def test_select_of_select(self):
        df = self.c.sql(
            """
            SELECT 2*c AS e, d - 1 AS f
            FROM
            (
                SELECT a - 1 AS c, 2*b  AS d
                FROM df
            ) AS `inner`
            """
        )
        df = df.compute()

        expected_df = pd.DataFrame(
            {"e": 2 * (self.df["a"] - 1), "f": 2 * self.df["b"] - 1}
        )
        assert_frame_equal(df, expected_df)
