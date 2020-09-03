from unittest import TestCase
import sqlite3

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import dask.dataframe as dd

from dask_sql import Context


class SQLLiteComparisonTestCase(TestCase):
    def setUp(self):
        np.random.seed(42)

        self.con = sqlite3.connect(":memory:")

        df1 = pd.DataFrame(
            {
                "user_id": np.random.choice([1, 2, 3, 4, float("nan")], 100),
                "a": np.random.rand(100),
                "b": np.random.randint(-10, 10, 100),
            }
        )

        df2 = pd.DataFrame(
            {
                "user_id": np.random.choice([1, 2, 3, 4], 100),
                "c": np.random.randint(20, 30, 100),
                "d": np.random.choice(["a", "b", "c"], 100),
            }
        )
        # the other is also a float, that makes joining simpler
        df2["user_id"] = df2["user_id"].astype("float64")

        self.c = Context()
        self.c.register_dask_table(dd.from_pandas(df1, npartitions=3), "df1")
        self.c.register_dask_table(dd.from_pandas(df2, npartitions=3), "df2")

        df1.to_sql("df1", self.con, index=False)
        df2.to_sql("df2", self.con, index=False)

    def assert_query_gives_same_result(self, query, sort_columns=None, **kwargs):
        sql_result = pd.read_sql_query(query, self.con)
        dask_result = self.c.sql(query, debug=True).compute()

        # allow that the names are differnet
        # as expressions are handled differently
        dask_result.columns = sql_result.columns

        if sort_columns:
            sql_result = sql_result.sort_values(sort_columns)
            dask_result = dask_result.sort_values(sort_columns)

        sql_result = sql_result.reset_index(drop=True)
        dask_result = dask_result.reset_index(drop=True)

        assert_frame_equal(sql_result, dask_result, **kwargs)

    def test_select(self):
        self.assert_query_gives_same_result(
            """
            SELECT * FROM df1
        """
        )

        self.assert_query_gives_same_result(
            """
            SELECT df1.user_id + 5 AS user_id, 2 * df1.b AS b FROM df1
        """
        )

        self.assert_query_gives_same_result(
            """
            SELECT df2.user_id, df2.d FROM df2
        """
        )

        self.assert_query_gives_same_result(
            """
            SELECT 1 AS I, -5.34344 AS F, 'öäll' AS S
        """,
            check_dtype=False,
        )

        self.assert_query_gives_same_result(
            """
            SELECT CASE WHEN user_id = 3 THEN 4 ELSE user_id END FROM df2
        """
        )

    def test_join(self):
        self.assert_query_gives_same_result(
            """
            SELECT
                df1.user_id, df1.a, df1.b,
                df2.user_id AS user_id_2, df2.c, df2.d
            FROM df1
            JOIN df2 ON df1.user_id = df2.user_id
        """,
            ["user_id", "a", "b", "user_id_2", "c", "d"],
        )

    def test_sort(self):
        self.assert_query_gives_same_result(
            """
            SELECT
                *
            FROM df1
            ORDER BY a, user_id, b DESC
        """
        )

        self.assert_query_gives_same_result(
            """
            SELECT
                c, d
            FROM df2
            ORDER BY c, d, user_id
        """
        )

    def test_limit(self):
        self.assert_query_gives_same_result(
            """
            SELECT
                c, d
            FROM df2
            ORDER BY c, d, user_id
            LIMIT 10 OFFSET 20
        """
        )

        self.assert_query_gives_same_result(
            """
            SELECT
                c, d
            FROM df2
            ORDER BY c, d, user_id
            LIMIT 200
        """
        )

    def test_groupby(self):
        self.assert_query_gives_same_result(
            """
            SELECT
                d, SUM(c), AVG(user_id)
            FROM df2
            GROUP BY d
            ORDER BY SUM(c)
            LIMIT 10
        """
        )

    def test_filter(self):
        self.assert_query_gives_same_result(
            """
            SELECT
                *
            FROM df1
            WHERE
                user_id = 3 AND a > 0.5
        """
        )

        self.assert_query_gives_same_result(
            """
            SELECT
                *
            FROM df2
            WHERE
                d LIKE '%c'
        """
        )
