from unittest import TestCase

import dask.dataframe as dd
import pandas as pd
from pandas.testing import assert_frame_equal

from dask_sql.context import Context


class DaskTestCase(TestCase):
    def setUp(self):
        self.df = pd.DataFrame({"a": [1, 2, 3], "b": [1.1, 2.2, 3.3]})
        dask_df = dd.from_pandas(self.df, npartitions=1)

        self.c = Context()
        self.c.register_dask_table(dask_df, "my_table")

    def test_select(self):
        df = self.c.sql("SELECT * from my_table")
        df = df.compute()

        assert_frame_equal(df, self.df)

    def test_select_column(self):
        df = self.c.sql("SELECT a from my_table")
        df = df.compute()

        assert_frame_equal(df, self.df[["a"]])

    def test_select_expr(self):
        df = self.c.sql("SELECT a + 1 AS a, b AS bla from my_table")
        df = df.compute()

        expected_df = pd.DataFrame({"a": self.df["a"] + 1, "bla": self.df["b"]})
        assert_frame_equal(df, expected_df)

    def test_filter(self):
        df = self.c.sql("SELECT * from my_table WHERE a < 2")
        df = df.compute()

        assert_frame_equal(df, self.df[self.df["a"] < 2])

    def test_filter_complicated(self):
        df = self.c.sql("SELECT * from my_table WHERE a < 3 AND (b > 1 AND b < 3)")
        df = df.compute()

        assert_frame_equal(df, self.df[((self.df["a"] < 3) & ((self.df["b"] > 1) & (self.df["b"] < 3)))])

    def test_join(self):
        df = self.c.sql("SELECT lhs.a, rhs.b from my_table AS lhs JOIN my_table AS rhs ON lhs.a = rhs.a")
        df = df.compute()

        assert_frame_equal(df, self.df)

    def test_join_inner(self):
        df = self.c.sql("SELECT lhs.a, rhs.b from my_table AS lhs INNER JOIN my_table AS rhs ON lhs.a = rhs.a")
        df = df.compute()

        assert_frame_equal(df, self.df)

    def test_join_outer(self):
        df = self.c.sql("SELECT lhs.a, rhs.b from my_table AS lhs FULL JOIN my_table AS rhs ON lhs.a = rhs.a")
        df = df.compute()

        assert_frame_equal(df, self.df)

    def test_join_left(self):
        df = self.c.sql("SELECT lhs.a, rhs.b from my_table AS lhs LEFT JOIN my_table AS rhs ON lhs.a = rhs.a")
        df = df.compute()

        assert_frame_equal(df, self.df)

    def test_join_right(self):
        df = self.c.sql("SELECT lhs.a, rhs.b from my_table AS lhs RIGHT JOIN my_table AS rhs ON lhs.a = rhs.a")
        df = df.compute()

        assert_frame_equal(df, self.df)

    def test_join_too_complex(self):
        self.assertRaises(NotImplementedError, self.c.sql, "SELECT lhs.a, rhs.b from my_table AS lhs JOIN my_table AS rhs ON lhs.a < rhs.b")

    def test_join_strange(self):
        self.assertRaises(NotImplementedError, self.c.sql, "SELECT lhs.a, rhs.b from my_table AS lhs JOIN my_table AS rhs ON lhs.a = 3")

    def test_join_complex(self):
        df = dd.from_pandas(pd.DataFrame({"user_id": [1, 1, 2], "b": [1, 2, 3]}), npartitions=1)
        df2 = dd.from_pandas(pd.DataFrame({"user_id": [2, 2, 2], "c": [3, 2, 1]}), npartitions=1)

        self.c.register_dask_table(df, "my_table")
        self.c.register_dask_table(df2, "my_table_2")

        df = self.c.sql("""
        SELECT
            lhs.user_id, lhs.b, rhs.user_id, rhs.c
        FROM my_table AS lhs
        JOIN my_table_2 AS rhs
            ON lhs.user_id = rhs.user_id AND lhs.b - rhs.c >= 0
        """)

        df = df.compute()

        df_expected = pd.DataFrame({
             "user_id": [2, 2, 2],
             "b": [3, 3, 3],
             "user_id": [2, 2, 2],
             "c": [3, 2, 1]
        })