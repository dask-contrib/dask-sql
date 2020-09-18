import numpy as np
import pandas as pd
import dask.dataframe as dd
from pandas.testing import assert_frame_equal

from tests.integration.fixtures import DaskTestCase


class RexOperationsTestCase(DaskTestCase):
    def test_case(self):
        df = self.c.sql(
            """
        SELECT
            (CASE WHEN a = 3 THEN 1 END) AS "S1",
            (CASE WHEN a > 0 THEN a ELSE 1 END) AS "S2",
            (CASE WHEN a = 4 THEN 3 ELSE a + 1 END) AS "S3",
            (CASE WHEN a = 3 THEN 1 ELSE a END) AS "S4"
        FROM df
        """
        )
        df = df.compute()

        expected_df = pd.DataFrame(index=self.df.index)
        expected_df["S1"] = self.df.a.apply(lambda a: 1 if a == 3 else None)
        expected_df["S2"] = self.df.a.apply(lambda a: a if a > 0 else 1)
        expected_df["S3"] = self.df.a.apply(lambda a: 3 if a == 4 else a + 1)
        expected_df["S4"] = self.df.a.apply(lambda a: 1 if a == 3 else a)
        assert_frame_equal(df, expected_df)

    def test_literals(self):
        df = self.c.sql(
            """SELECT 'a string äö' AS "S",
                      4.4 AS "F",
                      -4564347464 AS "I",
                      TIME '08:08:00.091' AS "T",
                      TIMESTAMP '2022-04-06 17:33:21' AS "DT",
                      DATE '1991-06-02' AS "D"
            """
        )
        df = df.compute()

        expected_df = pd.DataFrame(
            {
                "S": ["a string äö"],
                "F": [4.4],
                "I": [-4564347464],
                "T": [pd.to_datetime("1970-01-01 08:08:00.091+00:00")],
                "DT": [pd.to_datetime("2022-04-06 17:33:21+00:00")],
                "D": [pd.to_datetime("1991-06-02 00:00+00:00")],
            }
        )
        assert_frame_equal(df, expected_df)

    def test_literal_null(self):
        df = self.c.sql(
            """
        SELECT NULL AS "N", 1 + NULL AS "I"
        """
        )
        df = df.compute()

        expected_df = pd.DataFrame({"N": [None], "I": [np.nan]})
        assert_frame_equal(df, expected_df)

    def test_not(self):
        df = self.c.sql(
            """
        SELECT
            *
        FROM string_table
        WHERE NOT a LIKE '%normal%'
        """
        )
        df = df.compute()

        expected_df = self.string_table[~self.string_table.a.str.contains("normal")]
        assert_frame_equal(df, expected_df, check_dtype=False)

    def test_operators(self):
        df = self.c.sql(
            """
        SELECT
            a * b AS m,
            a / b AS q,
            a + b AS s,
            a - b AS d,
            a = b AS e,
            a > b AS g,
            a >= b AS ge,
            a < b AS l,
            a <= b AS le,
            a <> b AS n
        FROM df
        """,
            debug=True,
        )
        df = df.compute()

        expected_df = pd.DataFrame(index=self.df.index)
        expected_df["m"] = self.df["a"] * self.df["b"]
        expected_df["q"] = self.df["a"] / self.df["b"]
        expected_df["s"] = self.df["a"] + self.df["b"]
        expected_df["d"] = self.df["a"] - self.df["b"]
        expected_df["e"] = self.df["a"] == self.df["b"]
        expected_df["g"] = self.df["a"] > self.df["b"]
        expected_df["ge"] = self.df["a"] >= self.df["b"]
        expected_df["l"] = self.df["a"] < self.df["b"]
        expected_df["le"] = self.df["a"] <= self.df["b"]
        expected_df["n"] = self.df["a"] != self.df["b"]
        assert_frame_equal(df, expected_df)

    def test_like(self):
        df = self.c.sql(
            """
            SELECT * FROM string_table
            WHERE a LIKE '%n[a-z]rmal st_i%'
        """
        ).compute()

        assert_frame_equal(df, self.string_table.iloc[[0]])

        df = self.c.sql(
            """
            SELECT * FROM string_table
            WHERE a LIKE 'Ä%Ä_Ä%' ESCAPE 'Ä'
        """
        ).compute()

        assert_frame_equal(df, self.string_table.iloc[[1]])

        df = self.c.sql(
            """
            SELECT * FROM string_table
            WHERE a LIKE '^|()-*r[r]$' ESCAPE 'r'
        """
        ).compute()

        assert_frame_equal(df, self.string_table.iloc[[2]])

        df = self.c.sql(
            """
            SELECT * FROM string_table
            WHERE a LIKE '%_' ESCAPE 'r'
        """
        ).compute()

        assert_frame_equal(df, self.string_table)

    def test_null(self):
        df = self.c.sql(
            """
            SELECT
                c IS NOT NULL AS nn,
                c IS NULL AS n
            FROM user_table_nan
        """
        ).compute()

        expected_df = pd.DataFrame(index=[0, 1, 2])
        expected_df["nn"] = [True, False, True]
        expected_df["n"] = [False, True, False]
        assert_frame_equal(df, expected_df)

