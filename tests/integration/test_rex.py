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
        expected_df["S1"] = self.df.a.apply(lambda a: 1 if a == 3 else pd.NA)
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
                      DATE '1991-06-02' AS "D",
                      INTERVAL '1' DAY AS "IN"
            """
        )
        df = df.compute()

        expected_df = pd.DataFrame(
            {
                "S": ["a string äö"],
                "F": [4.4],
                "I": [-4564347464],
                "T": [pd.to_datetime("1970-01-01 08:08:00.091")],
                "DT": [pd.to_datetime("2022-04-06 17:33:21")],
                "D": [pd.to_datetime("1991-06-02 00:00")],
                "IN": [pd.to_timedelta("1d")],
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

        expected_df = pd.DataFrame({"N": [pd.NA], "I": [pd.NA]})
        expected_df["I"] = expected_df["I"].astype("Int32")
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
        assert_frame_equal(df, expected_df)

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
        """
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

        df = self.c.sql(
            """
            SELECT
                a IS NOT NULL AS nn,
                a IS NULL AS n
            FROM string_table
        """
        ).compute()

        expected_df = pd.DataFrame(index=[0, 1, 2])
        expected_df["nn"] = [True, True, True]
        expected_df["n"] = [False, False, False]
        assert_frame_equal(df, expected_df)

    def test_boolean_operations(self):
        df = dd.from_pandas(pd.DataFrame({"b": [1, 0, -1]}), npartitions=1)
        df["b"] = df["b"].apply(
            lambda x: pd.NA if x < 0 else x > 0, meta=("b", "bool")
        )  # turn into a bool column
        self.c.register_dask_table(df, "df")

        df = self.c.sql(
            """
            SELECT
                b IS TRUE AS t,
                b IS FALSE AS f,
                b IS NOT TRUE AS nt,
                b IS NOT FALSE AS nf,
                b IS UNKNOWN AS u,
                b IS NOT UNKNOWN AS nu
            FROM df"""
        ).compute()

        expected_df = pd.DataFrame(
            {
                "t": [True, False, False],
                "f": [False, True, False],
                "nt": [False, True, True],
                "nf": [True, False, True],
                "u": [False, False, True],
                "nu": [True, True, False],
            }
        )
        assert_frame_equal(df, expected_df)

    def test_math_operations(self):
        df = self.c.sql(
            """
            SELECT
                ABS(b) AS "abs"
                , ACOS(b) AS "acos"
                , ASIN(b) AS "asin"
                , ATAN(b) AS "atan"
                , ATAN2(a, b) AS "atan2"
                , CBRT(b) AS "cbrt"
                , CEIL(b) AS "ceil"
                , COS(b) AS "cos"
                , COT(b) AS "cot"
                , DEGREES(b) AS "degrees"
                , EXP(b) AS "exp"
                , FLOOR(b) AS "floor"
                , LOG10(b) AS "log10"
                , LN(b) AS "ln"
                , POWER(b, 2) AS "power"
                , POWER(b, a) AS "power2"
                , RADIANS(b) AS "radians"
                , ROUND(b) AS "round"
                , ROUND(b, 3) AS "round2"
                , SIGN(b) AS "sign"
                , SIN(b) AS "sin"
                , TAN(b) AS "tan"
                , TRUNCATE(b) AS "truncate"
            FROM df
        """
        ).compute()

        expected_df = pd.DataFrame(index=self.df.index)
        expected_df["abs"] = self.df.b.abs()
        expected_df["acos"] = np.arccos(self.df.b)
        expected_df["asin"] = np.arcsin(self.df.b)
        expected_df["atan"] = np.arctan(self.df.b)
        expected_df["atan2"] = np.arctan2(self.df.a, self.df.b)
        expected_df["cbrt"] = np.cbrt(self.df.b)
        expected_df["ceil"] = np.ceil(self.df.b)
        expected_df["cos"] = np.cos(self.df.b)
        expected_df["cot"] = 1 / np.tan(self.df.b)
        expected_df["degrees"] = self.df.b / np.pi * 180
        expected_df["exp"] = np.exp(self.df.b)
        expected_df["floor"] = np.floor(self.df.b)
        expected_df["log10"] = np.log10(self.df.b)
        expected_df["ln"] = np.log(self.df.b)
        expected_df["power"] = np.power(self.df.b, 2)
        expected_df["power2"] = np.power(self.df.b, self.df.a)
        expected_df["radians"] = self.df.b / 180 * np.pi
        expected_df["round"] = np.round(self.df.b)
        expected_df["round2"] = np.round(self.df.b, 3)
        expected_df["sign"] = np.sign(self.df.b)
        expected_df["sin"] = np.sin(self.df.b)
        expected_df["tan"] = np.tan(self.df.b)
        expected_df["truncate"] = np.trunc(self.df.b)
        assert_frame_equal(df, expected_df)

    def test_integer_div(self):
        df = self.c.sql(
            """
            SELECT
                1 / a AS a,
                a / 2 AS b,
                1.0 / a AS c
            FROM df_simple
        """
        ).compute()

        expected_df = pd.DataFrame(index=self.df_simple.index)
        expected_df["a"] = [1, 0, 0]
        expected_df["a"] = expected_df["a"].astype("Int64")
        expected_df["b"] = [0, 1, 1]
        expected_df["b"] = expected_df["b"].astype("Int64")
        expected_df["c"] = [1.0, 0.5, 0.333333]
        assert_frame_equal(df, expected_df)

    def test_subqueries(self):
        df = self.c.sql(
            """
            SELECT *
            FROM
                user_table_2
            WHERE
                EXISTS(
                    SELECT *
                    FROM user_table_1
                    WHERE
                        user_table_1.b = user_table_2.c
                )
        """
        ).compute()

        assert_frame_equal(
            df.reset_index(drop=True),
            self.user_table_2[
                self.user_table_2.c.isin(self.user_table_1.b)
            ].reset_index(drop=True),
        )

    def test_string_functions(self):
        df = self.c.sql(
            """
            SELECT
                a || 'hello' || a AS a,
                CHAR_LENGTH(a) AS b,
                UPPER(a) AS c,
                LOWER(a) AS d,
                POSITION('a' IN a FROM 4) AS e,
                POSITION('ZL' IN a) AS f,
                TRIM('a' FROM a) AS g,
                TRIM(BOTH 'a' FROM a) AS h,
                TRIM(LEADING 'a' FROM a) AS i,
                TRIM(TRAILING 'a' FROM a) AS j,
                OVERLAY(a PLACING 'XXX' FROM -1) AS k,
                OVERLAY(a PLACING 'XXX' FROM 2 FOR 4) AS l,
                OVERLAY(a PLACING 'XXX' FROM 2 FOR 1) AS m,
                SUBSTRING(a FROM -1) AS n,
                SUBSTRING(a FROM 10) AS o,
                SUBSTRING(a FROM 2) AS p,
                SUBSTRING(a FROM 2 FOR 2) AS q,
                INITCAP(a) AS r,
                INITCAP(UPPER(a)) AS s,
                INITCAP(LOWER(a)) AS t
            FROM
                string_table
            """
        ).compute()

        expected_df = pd.DataFrame(
            {
                "a": ["a normal stringhelloa normal string"],
                "b": [15],
                "c": ["A NORMAL STRING"],
                "d": ["a normal string"],
                "e": [7],
                "f": [0],
                "g": [" normal string"],
                "h": [" normal string"],
                "i": [" normal string"],
                "j": ["a normal string"],
                "k": ["XXXormal string"],
                "l": ["aXXXmal string"],
                "m": ["aXXXnormal string"],
                "n": ["a normal string"],
                "o": ["string"],
                "p": [" normal string"],
                "q": [" n"],
                "r": ["A Normal String"],
                "s": ["A Normal String"],
                "t": ["A Normal String"],
            }
        )

        assert_frame_equal(
            df.head(1), expected_df,
        )
