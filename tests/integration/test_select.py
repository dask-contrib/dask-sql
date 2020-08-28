import pandas as pd
from pandas.testing import assert_frame_equal

from tests.integration.fixtures import DaskTestCase


class SelectTestCase(DaskTestCase):
    def test_select(self):
        df = self.c.sql("SELECT * from df")
        df = df.compute()

        assert_frame_equal(df, self.df)

    def test_select_column(self):
        df = self.c.sql("SELECT a from df")
        df = df.compute()

        assert_frame_equal(df, self.df[["a"]])

    def test_select_expr(self):
        df = self.c.sql("SELECT a + 1 AS a, b AS bla from df")
        df = df.compute()

        expected_df = pd.DataFrame({"a": self.df["a"] + 1, "bla": self.df["b"]})
        assert_frame_equal(df, expected_df)
