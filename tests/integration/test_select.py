import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import dask.dataframe as dd

from dask_sql.utils import ParsingException
from tests.integration.fixtures import DaskTestCase


class SelectTestCase(DaskTestCase):
    def test_select(self):
        df = self.c.sql("SELECT * FROM df")
        df = df.compute()

        assert_frame_equal(df, self.df)

    def test_select_alias(self):
        df = self.c.sql("SELECT a as b, b as a FROM df")
        df = df.compute()

        expected_df = pd.DataFrame(index=self.df.index)
        expected_df["b"] = self.df.a
        expected_df["a"] = self.df.b

        assert_frame_equal(df[["a", "b"]], expected_df[["a", "b"]])

    def test_select_column(self):
        df = self.c.sql("SELECT a FROM df")
        df = df.compute()

        assert_frame_equal(df, self.df[["a"]])

    def test_select_different_types(self):
        expected_df = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    ["2022-01-21 17:34", "2022-01-21", "17:34", pd.NaT]
                ),
                "string": ["this is a test", "another test", "äölüć", ""],
                "integer": [1, 2, -4, 5],
                "float": [-1.1, np.NaN, pd.NA, np.sqrt(2)],
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
        df = self.c.sql("SELECT a + 1 AS a, b AS bla, a - 1 FROM df")
        df = df.compute()

        expected_df = pd.DataFrame(
            {
                "a": self.df["a"] + 1,
                "bla": self.df["b"],
                '"df"."a" - 1': self.df["a"] - 1,
            }
        )
        assert_frame_equal(df, expected_df)

    def test_select_of_select(self):
        df = self.c.sql(
            """
            SELECT 2*c AS e, d - 1 AS f
            FROM
            (
                SELECT a - 1 AS c, 2*b  AS d
                FROM df
            ) AS "inner"
            """
        )
        df = df.compute()

        expected_df = pd.DataFrame(
            {"e": 2 * (self.df["a"] - 1), "f": 2 * self.df["b"] - 1}
        )
        assert_frame_equal(df, expected_df)

    def test_wrong_input(self):
        self.assertRaises(ParsingException, self.c.sql, """SELECT x FROM df""")
        self.assertRaises(ParsingException, self.c.sql, """SELECT x FROM df""")
