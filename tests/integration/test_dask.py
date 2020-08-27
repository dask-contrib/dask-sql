from unittest import TestCase

import dask.dataframe as dd
import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np

from dask_sql.context import Context


class DaskTestCase(TestCase):
    def setUp(self):
        self.df = pd.DataFrame({"a": [1, 2, 3], "b": [1.1, 2.2, 3.3]})
        self.df2 = pd.DataFrame({"user_id": [2, 1, 2], "c": [3, 3, 1]})
        self.df3 = pd.DataFrame({"user_id": [1, 1, 2], "b": [1, 2, 3]})
        self.df4 = pd.DataFrame({"user_id": [2, 2, 2], "c": [3, 2, 1]})
        self.df5 = pd.DataFrame({"a": [0] * 100 + [1] * 101 + [2] * 103})

        self.c = Context()
        self.c.register_dask_table(dd.from_pandas(self.df, npartitions=3), "my_table")
        self.c.register_dask_table(
            dd.from_pandas(self.df2, npartitions=3), "my_table_2"
        )
        self.c.register_dask_table(
            dd.from_pandas(self.df3, npartitions=3), "my_table_3"
        )
        self.c.register_dask_table(
            dd.from_pandas(self.df4, npartitions=3), "my_table_4"
        )
        self.c.register_dask_table(
            dd.from_pandas(self.df5, npartitions=3), "my_table_5"
        )

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

        assert_frame_equal(
            df,
            self.df[((self.df["a"] < 3) & ((self.df["b"] > 1) & (self.df["b"] < 3)))],
        )

    def test_sort(self):
        df = self.c.sql(
            """
        SELECT
            *
        FROM my_table_2
        ORDER BY c, user_id DESC
        """
        )
        df = df.compute().reset_index(drop=True)
        df_expected = self.df2.sort_values(
            ["c", "user_id"], ascending=[True, False]
        ).reset_index(drop=True)

        assert_frame_equal(df, df_expected)

    def test_sort_not_allowed(self):
        self.assertRaises(
            NotImplementedError, self.c.sql, "SELECT * FROM my_table_2 ORDER BY c DESC"
        )

    def test_join(self):
        df = self.c.sql(
            "SELECT lhs.a, rhs.b from my_table AS lhs JOIN my_table AS rhs ON lhs.a = rhs.a"
        )
        df = df.compute()

        assert_frame_equal(df.reset_index(drop=True), self.df)

    def test_join_inner(self):
        df = self.c.sql(
            "SELECT lhs.a, rhs.b from my_table AS lhs INNER JOIN my_table AS rhs ON lhs.a = rhs.a"
        )
        df = df.compute()

        assert_frame_equal(df.reset_index(drop=True), self.df)

    def test_join_outer(self):
        df = self.c.sql(
            "SELECT lhs.a, rhs.b from my_table AS lhs FULL JOIN my_table AS rhs ON lhs.a = rhs.a"
        )
        df = df.compute()

        assert_frame_equal(df.reset_index(drop=True), self.df)

    def test_join_left(self):
        df = self.c.sql(
            "SELECT lhs.a, rhs.b from my_table AS lhs LEFT JOIN my_table AS rhs ON lhs.a = rhs.a"
        )
        df = df.compute()

        assert_frame_equal(df.reset_index(drop=True), self.df)

    def test_join_right(self):
        df = self.c.sql(
            "SELECT lhs.a, rhs.b from my_table AS lhs RIGHT JOIN my_table AS rhs ON lhs.a = rhs.a"
        )
        df = df.compute()

        assert_frame_equal(df.reset_index(drop=True), self.df)

    def test_join_strange(self):
        self.assertRaises(
            NotImplementedError,
            self.c.sql,
            "SELECT lhs.a, rhs.b from my_table AS lhs JOIN my_table AS rhs ON lhs.a = 3",
        )

    def test_join_complex(self):
        df = self.c.sql(
            "SELECT lhs.a, rhs.b from my_table AS lhs JOIN my_table AS rhs ON lhs.a < rhs.b",
        )
        df = df.compute()

        df_expected = pd.DataFrame(
            {"a": [1, 1, 1, 2, 2, 3], "b": [1.1, 2.2, 3.3, 2.2, 3.3, 3.3]}
        )

        assert_frame_equal(
            df.sort_values(["a", "b"]).reset_index(drop=True), df_expected
        )

    def test_join_complex_2(self):
        df = self.c.sql(
            """
        SELECT
            lhs.user_id, lhs.b, rhs.user_id, rhs.c
        FROM my_table_3 AS lhs
        JOIN my_table_4 AS rhs
            ON lhs.user_id = rhs.user_id AND lhs.b - rhs.c >= 0
        """
        )

        df = df.compute()

        df_expected = pd.DataFrame(
            {
                "user_id": [2, 2, 2],
                "b": [3, 3, 3],
                "user_id0": [2, 2, 2],
                "c": [1, 2, 3],
            }
        )

        assert_frame_equal(df.sort_values("c").reset_index(drop=True), df_expected)

    def test_limit(self):
        df = self.c.sql("SELECT * FROM my_table_5 LIMIT 101")
        df = df.compute()

        assert_frame_equal(df, self.df5.iloc[:101])

        df = self.c.sql("SELECT * FROM my_table_5 LIMIT 100")
        df = df.compute()

        assert_frame_equal(df, self.df5.iloc[:100])

        df = self.c.sql("SELECT * FROM my_table_5 LIMIT 100 OFFSET 99")
        df = df.compute()

        assert_frame_equal(df, self.df5.iloc[99 : 99 + 100])

        df = self.c.sql("SELECT * FROM my_table_5 LIMIT 100 OFFSET 100")
        df = df.compute()

        assert_frame_equal(df, self.df5.iloc[100 : 100 + 100])

        df = self.c.sql("SELECT * FROM my_table_5 LIMIT 101 OFFSET 101")
        df = df.compute()

        assert_frame_equal(df, self.df5.iloc[101 : 101 + 101])

    def test_group_by(self):
        df = self.c.sql(
            """
        SELECT
            user_id, SUM(c) AS S
        FROM my_table_2
        GROUP BY user_id
        """
        )
        df = df.compute()

        expected_df = pd.DataFrame({"user_id": [2, 1], "S": [4, 3]})
        assert_frame_equal(df, expected_df)

    def test_group_by_all(self):
        df = self.c.sql(
            """
        SELECT
            SUM(c) AS S, SUM(2) AS X
        FROM my_table_2
        """
        )
        df = df.compute()

        expected_df = pd.DataFrame({"S": [7], "X": [6]})
        assert_frame_equal(df, expected_df, check_dtype=False)

    def test_group_by_case(self):
        df = self.c.sql(
            """
        SELECT
            user_id, SUM(CASE WHEN c = 3 THEN 1 END) AS S
        FROM my_table_2
        GROUP BY user_id
        """
        )
        df = df.compute()

        expected_df = pd.DataFrame({"user_id": [2, 1], "S": [1, 1]})
        assert_frame_equal(df, expected_df)

    def test_case(self):
        df = self.c.sql(
            """
        SELECT
            (CASE WHEN a = 3 THEN 1 END) AS S1,
            (CASE WHEN a > 0 THEN a ELSE 1 END) AS S2,
            (CASE WHEN a = 4 THEN 3 ELSE a + 1 END) AS S3,
            (CASE WHEN a = 3 THEN 1 ELSE a END) AS S4
        FROM my_table
        """
        )
        df = df.compute()

        expected_df = pd.DataFrame(
            {"S1": [None, None, 1], "S2": [1, 2, 3], "S3": [2, 3, 4], "S4": [1, 2, 1]}
        )
        assert_frame_equal(df, expected_df, check_dtype=False)

    def test_literals(self):
        df = self.c.sql("""SELECT 'a string' as S, 4.4 AS F, -456434 AS I """)
        df = df.compute()

        expected_df = pd.DataFrame({"S": ["a string"], "F": [4.4], "I": [-456434]})
        assert_frame_equal(df, expected_df, check_dtype=False)
