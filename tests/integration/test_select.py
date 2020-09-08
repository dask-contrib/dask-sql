import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import dask.dataframe as dd

from tests.integration.fixtures import DaskTestCase


class SelectTestCase(DaskTestCase):
    def test_select(self):
        df = self.c.sql("SELECT * FROM df")
        df = df.compute()

        assert_frame_equal(df, self.df)

    def test_select_alias(self):
        df = self.c.sql("SELECT a as b, b as a FROM df")
        df = df.compute()

        expected_df = self.df
        expected_df.assign(a=self.df.b, b=self.df.a)

        assert_frame_equal(df, expected_df)

    def test_select_column(self):
        df = self.c.sql("SELECT a FROM df")
        df = df.compute()

        assert_frame_equal(df, self.df[["a"]])

    def test_select_different_types(self):
        expected_df = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    ["2022-01-21 17:34", "2022-01-21", "17:34", np.NaN]
                ),
                "string": ["this is a test", "another test", "äölüć", ""],
                "integer": [1, 2, -4, 5],
                "float": [-1.1, np.pi, np.NaN, np.sqrt(2)],
            }
        )
        self.c.register_dask_table(dd.from_pandas(expected_df, npartitions=1), "df")
        df = self.c.sql(
            """
        SELECT *
        FROM df
        """
        )
        df = df.compute()

        assert_frame_equal(df, expected_df)

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
