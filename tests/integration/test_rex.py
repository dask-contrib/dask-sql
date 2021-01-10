from datetime import datetime

import pytest
import numpy as np
import pandas as pd
import dask.dataframe as dd
from pandas.testing import assert_frame_equal


def test_case(c, df):
    result_df = c.sql(
        """
    SELECT
        (CASE WHEN a = 3 THEN 1 END) AS "S1",
        (CASE WHEN a > 0 THEN a ELSE 1 END) AS "S2",
        (CASE WHEN a = 4 THEN 3 ELSE a + 1 END) AS "S3",
        (CASE WHEN a = 3 THEN 1 ELSE a END) AS "S4"
    FROM df
    """
    )
    result_df = result_df.compute()

    expected_df = pd.DataFrame(index=df.index)
    expected_df["S1"] = df.a.apply(lambda a: 1 if a == 3 else pd.NA)
    expected_df["S2"] = df.a.apply(lambda a: a if a > 0 else 1)
    expected_df["S3"] = df.a.apply(lambda a: 3 if a == 4 else a + 1)
    expected_df["S4"] = df.a.apply(lambda a: 1 if a == 3 else a)
    assert_frame_equal(result_df, expected_df)


def test_literals(c):
    df = c.sql(
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


def test_literal_null(c):
    df = c.sql(
        """
    SELECT NULL AS "N", 1 + NULL AS "I"
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame({"N": [pd.NA], "I": [pd.NA]})
    expected_df["I"] = expected_df["I"].astype("Int32")
    assert_frame_equal(df, expected_df)


def test_random(c, df):
    result_df = c.sql(
        """
    SELECT RAND(0) AS "0", RAND_INTEGER(1, 10) AS "1"
    """
    )
    result_df = result_df.compute()

    # As the seed is fixed, this should always give the same results
    expected_df = pd.DataFrame({"0": [0.26183678695392976], "1": [8]})
    expected_df["1"] = expected_df["1"].astype("Int32")
    assert_frame_equal(result_df, expected_df)

    result_df = c.sql(
        """
    SELECT RAND(42) AS "R" FROM df WHERE RAND(0) < b
    """
    )
    result_df = result_df.compute()

    assert len(result_df) == 659
    assert list(result_df["R"].iloc[:5]) == [
        0.5276488824980542,
        0.17861463145673728,
        0.33764733440490524,
        0.6590485298464198,
        0.08554137165307785,
    ]

    # If we do not fix the seed, we can just test if it works at all
    result_df = c.sql(
        """
    SELECT RAND() AS "0", RAND_INTEGER(10) AS "1"
    """
    )
    result_df = result_df.compute()


def test_not(c, string_table):
    df = c.sql(
        """
    SELECT
        *
    FROM string_table
    WHERE NOT a LIKE '%normal%'
    """
    )
    df = df.compute()

    expected_df = string_table[~string_table.a.str.contains("normal")]
    assert_frame_equal(df, expected_df)


def test_operators(c, df):
    result_df = c.sql(
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
    result_df = result_df.compute()

    expected_df = pd.DataFrame(index=df.index)
    expected_df["m"] = df["a"] * df["b"]
    expected_df["q"] = df["a"] / df["b"]
    expected_df["s"] = df["a"] + df["b"]
    expected_df["d"] = df["a"] - df["b"]
    expected_df["e"] = df["a"] == df["b"]
    expected_df["g"] = df["a"] > df["b"]
    expected_df["ge"] = df["a"] >= df["b"]
    expected_df["l"] = df["a"] < df["b"]
    expected_df["le"] = df["a"] <= df["b"]
    expected_df["n"] = df["a"] != df["b"]
    assert_frame_equal(result_df, expected_df)


def test_like(c, string_table):
    df = c.sql(
        """
        SELECT * FROM string_table
        WHERE a SIMILAR TO '%n[a-z]rmal st_i%'
    """
    ).compute()

    assert_frame_equal(df, string_table.iloc[[0]])

    df = c.sql(
        """
        SELECT * FROM string_table
        WHERE a LIKE '%n[a-z]rmal st_i%'
    """
    ).compute()

    assert len(df) == 0

    df = c.sql(
        """
        SELECT * FROM string_table
        WHERE a LIKE 'Ä%Ä_Ä%' ESCAPE 'Ä'
    """
    ).compute()

    assert_frame_equal(df, string_table.iloc[[1]])

    df = c.sql(
        """
        SELECT * FROM string_table
        WHERE a SIMILAR TO '^|()-*r[r]$' ESCAPE 'r'
        """
    ).compute()

    assert_frame_equal(df, string_table.iloc[[2]])

    df = c.sql(
        """
        SELECT * FROM string_table
        WHERE a LIKE '^|()-*r[r]$' ESCAPE 'r'
    """
    ).compute()

    assert_frame_equal(df, string_table.iloc[[2]])

    df = c.sql(
        """
        SELECT * FROM string_table
        WHERE a LIKE '%_' ESCAPE 'r'
    """
    ).compute()

    assert_frame_equal(df, string_table)

    string_table2 = pd.DataFrame({"b": ["a", "b", None, pd.NA, float("nan")]})
    c.register_dask_table(dd.from_pandas(string_table2, npartitions=1), "string_table2")
    df = c.sql(
        """
        SELECT * FROM string_table2
        WHERE b LIKE 'b'
    """
    ).compute()

    assert_frame_equal(df, string_table2.iloc[[1]])


def test_null(c):
    df = c.sql(
        """
        SELECT
            c IS NOT NULL AS nn,
            c IS NULL AS n
        FROM user_table_nan
    """
    ).compute()

    expected_df = pd.DataFrame(index=[0, 1, 2])
    expected_df["nn"] = [True, False, True]
    expected_df["nn"] = expected_df["nn"].astype("boolean")
    expected_df["n"] = [False, True, False]
    assert_frame_equal(df, expected_df)

    df = c.sql(
        """
        SELECT
            a IS NOT NULL AS nn,
            a IS NULL AS n
        FROM string_table
    """
    ).compute()

    expected_df = pd.DataFrame(index=[0, 1, 2])
    expected_df["nn"] = [True, True, True]
    expected_df["nn"] = expected_df["nn"].astype("boolean")
    expected_df["n"] = [False, False, False]
    assert_frame_equal(df, expected_df)


def test_boolean_operations(c):
    df = dd.from_pandas(pd.DataFrame({"b": [1, 0, -1]}), npartitions=1)
    df["b"] = df["b"].apply(
        lambda x: pd.NA if x < 0 else x > 0, meta=("b", "bool")
    )  # turn into a bool column
    c.create_table("df", df)

    df = c.sql(
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
        },
        dtype="bool",
    )
    expected_df["nt"] = expected_df["nt"].astype("boolean")
    expected_df["nf"] = expected_df["nf"].astype("boolean")
    expected_df["nu"] = expected_df["nu"].astype("boolean")
    assert_frame_equal(df, expected_df)


def test_math_operations(c, df):
    result_df = c.sql(
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

    expected_df = pd.DataFrame(index=df.index)
    expected_df["abs"] = df.b.abs()
    expected_df["acos"] = np.arccos(df.b)
    expected_df["asin"] = np.arcsin(df.b)
    expected_df["atan"] = np.arctan(df.b)
    expected_df["atan2"] = np.arctan2(df.a, df.b)
    expected_df["cbrt"] = np.cbrt(df.b)
    expected_df["ceil"] = np.ceil(df.b)
    expected_df["cos"] = np.cos(df.b)
    expected_df["cot"] = 1 / np.tan(df.b)
    expected_df["degrees"] = df.b / np.pi * 180
    expected_df["exp"] = np.exp(df.b)
    expected_df["floor"] = np.floor(df.b)
    expected_df["log10"] = np.log10(df.b)
    expected_df["ln"] = np.log(df.b)
    expected_df["power"] = np.power(df.b, 2)
    expected_df["power2"] = np.power(df.b, df.a)
    expected_df["radians"] = df.b / 180 * np.pi
    expected_df["round"] = np.round(df.b)
    expected_df["round2"] = np.round(df.b, 3)
    expected_df["sign"] = np.sign(df.b)
    expected_df["sin"] = np.sin(df.b)
    expected_df["tan"] = np.tan(df.b)
    expected_df["truncate"] = np.trunc(df.b)
    assert_frame_equal(result_df, expected_df)


def test_integer_div(c, df_simple):
    df = c.sql(
        """
        SELECT
            1 / a AS a,
            a / 2 AS b,
            1.0 / a AS c
        FROM df_simple
    """
    ).compute()

    expected_df = pd.DataFrame(index=df_simple.index)
    expected_df["a"] = [1, 0, 0]
    expected_df["a"] = expected_df["a"].astype("Int64")
    expected_df["b"] = [0, 1, 1]
    expected_df["b"] = expected_df["b"].astype("Int64")
    expected_df["c"] = [1.0, 0.5, 0.333333]
    assert_frame_equal(df, expected_df)


def test_subqueries(c, user_table_1, user_table_2):
    df = c.sql(
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
        user_table_2[user_table_2.c.isin(user_table_1.b)].reset_index(drop=True),
    )


def test_string_functions(c):
    df = c.sql(
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


def test_date_functions(c):
    date = datetime(2021, 10, 3, 15, 53, 42, 47)

    df = dd.from_pandas(pd.DataFrame({"d": [date]}), npartitions=1)
    c.register_dask_table(df, "df")

    df = c.sql(
        """
        SELECT
            EXTRACT(CENTURY FROM d) AS "century",
            EXTRACT(DAY FROM d) AS "day",
            EXTRACT(DECADE FROM d) AS "decade",
            EXTRACT(DOW FROM d) AS "dow",
            EXTRACT(DOY FROM d) AS "doy",
            EXTRACT(HOUR FROM d) AS "hour",
            EXTRACT(MICROSECOND FROM d) AS "microsecond",
            EXTRACT(MILLENNIUM FROM d) AS "millennium",
            EXTRACT(MILLISECOND FROM d) AS "millisecond",
            EXTRACT(MINUTE FROM d) AS "minute",
            EXTRACT(MONTH FROM d) AS "month",
            EXTRACT(QUARTER FROM d) AS "quarter",
            EXTRACT(SECOND FROM d) AS "second",
            EXTRACT(WEEK FROM d) AS "week",
            EXTRACT(YEAR FROM d) AS "year",

            LAST_DAY(d) as "last_day",

            TIMESTAMPADD(YEAR, 2, d) as "plus_1_year",
            TIMESTAMPADD(MONTH, 1, d) as "plus_1_month",
            TIMESTAMPADD(WEEK, 1, d) as "plus_1_week",
            TIMESTAMPADD(DAY, 1, d) as "plus_1_day",
            TIMESTAMPADD(HOUR, 1, d) as "plus_1_hour",
            TIMESTAMPADD(MINUTE, 1, d) as "plus_1_min",
            TIMESTAMPADD(SECOND, 1, d) as "plus_1_sec",
            TIMESTAMPADD(MICROSECOND, 1000, d) as "plus_1000_millisec",
            TIMESTAMPADD(QUARTER, 1, d) as "plus_1_qt",

            CEIL(d TO DAY) as ceil_to_day,
            CEIL(d TO HOUR) as ceil_to_hour,
            CEIL(d TO MINUTE) as ceil_to_minute,
            CEIL(d TO SECOND) as ceil_to_seconds,
            CEIL(d TO MILLISECOND) as ceil_to_millisec,

            FLOOR(d TO DAY) as floor_to_day,
            FLOOR(d TO HOUR) as floor_to_hour,
            FLOOR(d TO MINUTE) as floor_to_minute,
            FLOOR(d TO SECOND) as floor_to_seconds,
            FLOOR(d TO MILLISECOND) as floor_to_millisec

        FROM df
    """
    ).compute()

    expected_df = pd.DataFrame(
        {
            "century": [20],
            "day": [3],
            "decade": [202],
            "dow": [0],
            "doy": [276],
            "hour": [15],
            "microsecond": [47],
            "millennium": [2],
            "millisecond": [47000],
            "minute": [53],
            "month": [10],
            "quarter": [4],
            "second": [42],
            "week": [39],
            "year": [2021],
            "last_day": [datetime(2021, 10, 31, 15, 53, 42, 47)],
            "plus_1_year": [datetime(2023, 10, 3, 15, 53, 42, 47)],
            "plus_1_month": [datetime(2021, 11, 3, 15, 53, 42, 47)],
            "plus_1_week": [datetime(2021, 10, 10, 15, 53, 42, 47)],
            "plus_1_day": [datetime(2021, 10, 4, 15, 53, 42, 47)],
            "plus_1_hour": [datetime(2021, 10, 3, 16, 53, 42, 47)],
            "plus_1_min": [datetime(2021, 10, 3, 15, 54, 42, 47)],
            "plus_1_sec": [datetime(2021, 10, 3, 15, 53, 43, 47)],
            "plus_1000_millisec": [datetime(2021, 10, 3, 15, 53, 42, 1047)],
            "plus_1_qt": [datetime(2022, 1, 3, 15, 53, 42, 47)],
            "ceil_to_day": [datetime(2021, 10, 4)],
            "ceil_to_hour": [datetime(2021, 10, 3, 16)],
            "ceil_to_minute": [datetime(2021, 10, 3, 15, 54)],
            "ceil_to_seconds": [datetime(2021, 10, 3, 15, 53, 43)],
            "ceil_to_millisec": [datetime(2021, 10, 3, 15, 53, 42, 1000)],
            "floor_to_day": [datetime(2021, 10, 3)],
            "floor_to_hour": [datetime(2021, 10, 3, 15)],
            "floor_to_minute": [datetime(2021, 10, 3, 15, 53)],
            "floor_to_seconds": [datetime(2021, 10, 3, 15, 53, 42)],
            "floor_to_millisec": [datetime(2021, 10, 3, 15, 53, 42)],
        }
    )

    assert_frame_equal(df, expected_df, check_dtype=False)

    # test exception handling
    with pytest.raises(NotImplementedError):
        df = c.sql(
            """
            SELECT
                FLOOR(d TO YEAR) as floor_to_year
            FROM df
            """
        ).compute()
