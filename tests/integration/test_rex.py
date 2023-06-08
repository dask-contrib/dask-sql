from datetime import datetime

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from dask_sql._compat import DASK_CUDF_TODATETIME_SUPPORT
from tests.utils import assert_eq


def test_year(c, datetime_table):
    result_df = c.sql(
        """
    SELECT year(timezone) from datetime_table
    """
    )
    assert result_df.shape[0].compute() == datetime_table.shape[0]
    assert result_df.compute().iloc[0][0] == 2014


def test_case(c, df):
    result_df = c.sql(
        """
    SELECT
        (CASE WHEN a = 3 THEN 1 END) AS "S1",
        (CASE WHEN a > 0 THEN a ELSE 1 END) AS "S2",
        (CASE WHEN a = 4 THEN 3 ELSE a + 1 END) AS "S3",
        (CASE WHEN a = 3 THEN 1 WHEN a > 0 THEN 2 ELSE a END) AS "S4",
        CASE
            WHEN (a >= 1 AND a < 2) OR (a > 2) THEN CAST('in-between' AS VARCHAR) ELSE CAST('out-of-range' AS VARCHAR)
        END AS "S5",
        CASE
            WHEN (a < 2) OR (3 < a AND a < 4) THEN 42 ELSE 47
        END AS "S6",
        CASE WHEN (1 < a AND a <= 4) THEN 1 ELSE 0 END AS "S7",
        CASE a WHEN 2 THEN 5 ELSE a + 1 END AS "S8"
    FROM df
    """
    )
    expected_df = pd.DataFrame(index=df.index)
    expected_df["S1"] = df.a.apply(lambda a: 1 if a == 3 else pd.NA)
    expected_df["S2"] = df.a.apply(lambda a: a if a > 0 else 1)
    expected_df["S3"] = df.a.apply(lambda a: 3 if a == 4 else a + 1)
    expected_df["S4"] = df.a.apply(lambda a: 1 if a == 3 else 2 if a > 0 else a)
    expected_df["S5"] = df.a.apply(
        lambda a: "in-between" if ((1 <= a < 2) or (a > 2)) else "out-of-range"
    )
    expected_df["S6"] = df.a.apply(lambda a: 42 if ((a < 2) or (3 < a < 4)) else 47)
    expected_df["S7"] = df.a.apply(lambda a: 1 if (1 < a <= 4) else 0)
    expected_df["S8"] = df.a.apply(lambda a: 5 if a == 2 else a + 1)

    # Do not check dtypes, as pandas versions are inconsistent here
    assert_eq(result_df, expected_df, check_dtype=False)


def test_intervals(c):
    df = c.sql(
        """SELECT INTERVAL '3' DAY as "IN"
        """
    )
    expected_df = pd.DataFrame(
        {
            "IN": [pd.to_timedelta("3d")],
        }
    )
    assert_eq(df, expected_df)

    date1 = datetime(2021, 10, 3, 15, 53, 42, 47)
    date2 = datetime(2021, 2, 28, 15, 53, 42, 47)
    dates = dd.from_pandas(pd.DataFrame({"d": [date1, date2]}), npartitions=1)
    c.create_table("dates", dates)
    df = c.sql(
        """SELECT d + INTERVAL '5 days' AS "Plus_5_days" FROM dates
        """
    )
    expected_df = pd.DataFrame(
        {
            "Plus_5_days": [
                datetime(2021, 10, 8, 15, 53, 42, 47),
                datetime(2021, 3, 5, 15, 53, 42, 47),
            ]
        }
    )
    assert_eq(df, expected_df)


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
    assert_eq(df, expected_df)


def test_date_interval_math(c):
    df = c.sql(
        """SELECT
                DATE '1998-08-18' - INTERVAL '4 days' AS "before",
                DATE '1998-08-18' + INTERVAL '4 days' AS "after"
        """
    )

    expected_df = pd.DataFrame(
        {
            "before": [pd.to_datetime("1998-08-14 00:00")],
            "after": [pd.to_datetime("1998-08-22 00:00")],
        }
    )
    assert_eq(df, expected_df)


def test_literal_null(c):
    df = c.sql(
        """
    SELECT NULL AS "N", 1 + NULL AS "I"
    """
    )

    expected_df = pd.DataFrame({"N": [pd.NA], "I": [pd.NA]})
    expected_df["I"] = expected_df["I"].astype("Int64")
    assert_eq(df, expected_df)


def test_random(c):
    query_with_seed = """
            SELECT
                RAND(0) AS "0",
                RAND_INTEGER(0, 10) AS "1"
            """

    result_df = c.sql(query_with_seed)

    # assert that repeated queries give the same result
    assert_eq(result_df, c.sql(query_with_seed))

    # assert output
    result_df = result_df.compute()

    assert result_df["0"].dtype == "float64"
    assert result_df["1"].dtype == "Int64"

    assert 0 <= result_df["0"][0] < 1
    assert 0 <= result_df["1"][0] < 10

    query_wo_seed = """
        SELECT
            RAND() AS "0",
            RANDOM() AS "1",
            RAND_INTEGER(30) AS "2"
        """
    result_df = c.sql(query_wo_seed)
    result_df = result_df.compute()
    # assert output types

    assert result_df["0"].dtype == "float64"
    assert result_df["1"].dtype == "float64"
    assert result_df["2"].dtype == "Int64"

    assert 0 <= result_df["0"][0] < 1
    assert 0 <= result_df["1"][0] < 1
    assert 0 <= result_df["2"][0] < 30


@pytest.mark.parametrize(
    "input_table",
    [
        "string_table",
        pytest.param("gpu_string_table", marks=pytest.mark.gpu),
    ],
)
def test_not(c, input_table, request):
    string_table = request.getfixturevalue(input_table)
    df = c.sql(
        f"""
    SELECT
        *
    FROM {input_table}
    WHERE NOT a LIKE '%normal%'
    """
    )

    expected_df = string_table[~string_table.a.str.contains("normal")]
    assert_eq(df, expected_df)


def test_operators(c, df):
    result_df = c.sql(
        """
    SELECT
        a * b AS m,
        -a AS u,
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

    expected_df = pd.DataFrame(index=df.index)
    expected_df["m"] = df["a"] * df["b"]
    expected_df["u"] = -df["a"]
    expected_df["q"] = df["a"] / df["b"]
    expected_df["s"] = df["a"] + df["b"]
    expected_df["d"] = df["a"] - df["b"]
    expected_df["e"] = df["a"] == df["b"]
    expected_df["g"] = df["a"] > df["b"]
    expected_df["ge"] = df["a"] >= df["b"]
    expected_df["l"] = df["a"] < df["b"]
    expected_df["le"] = df["a"] <= df["b"]
    expected_df["n"] = df["a"] != df["b"]
    assert_eq(result_df, expected_df)


@pytest.mark.parametrize(
    "input_table,gpu",
    [
        ("string_table", False),
        pytest.param(
            "gpu_string_table",
            True,
            marks=(
                pytest.mark.gpu,
                pytest.mark.xfail(
                    reason="Failing due to cuDF bug https://github.com/rapidsai/cudf/issues/9434"
                ),
            ),
        ),
    ],
)
def test_like(c, input_table, gpu, request):
    string_table = request.getfixturevalue(input_table)

    df = c.sql(
        f"""
        SELECT * FROM {input_table}
        WHERE a SIMILAR TO '%n[a-z]rmal st_i%'
    """
    )
    assert_eq(df, string_table.iloc[[0, 3]])

    df = c.sql(
        f"""
        SELECT * FROM {input_table}
        WHERE a NOT SIMILAR TO '%n[a-z]rmal st_i%'
    """
    )
    assert_eq(df, string_table.iloc[[1, 2]])

    df = c.sql(
        f"""
        SELECT * FROM {input_table}
        WHERE a LIKE '%n[a-z]rmal st_i%'
    """
    )
    assert len(df) == 0

    df = c.sql(
        f"""
        SELECT * FROM {input_table}
        WHERE a NOT LIKE '%n[a-z]rmal st_i%'
    """
    )
    assert_eq(df, string_table)

    df = c.sql(
        f"""
        SELECT * FROM {input_table}
        WHERE a LIKE '%a Normal String%'
    """
    )
    assert len(df) == 0

    df = c.sql(
        f"""
        SELECT * FROM {input_table}
        WHERE a ILIKE '%a Normal String%'
    """
    )
    assert_eq(df, string_table.iloc[[0, 3]])

    df = c.sql(
        f"""
        SELECT * FROM {input_table}
        WHERE a NOT ILIKE '%a Normal String%'
    """
    )
    assert_eq(df, string_table.iloc[[1, 2]])
    # TODO: uncomment when sqlparser adds parsing support for non-standard escape characters
    # https://github.com/dask-contrib/dask-sql/issues/754
    # df = c.sql(
    #     f"""
    #     SELECT * FROM {input_table}
    #     WHERE a LIKE 'Ä%Ä_Ä%' ESCAPE 'Ä'
    # """
    # )

    # assert_eq(df, string_table.iloc[[1]])

    df = c.sql(
        f"""
        SELECT * FROM {input_table}
        WHERE a SIMILAR TO '^|()-*r[r]$' ESCAPE 'r'
        """
    )

    assert_eq(df, string_table.iloc[[2, 3]])

    df = c.sql(
        f"""
        SELECT * FROM {input_table}
        WHERE a LIKE '^|()-*r[r]$' ESCAPE 'r'
    """
    )

    assert_eq(df, string_table.iloc[[2]])

    df = c.sql(
        f"""
        SELECT * FROM {input_table}
        WHERE a LIKE '%_' ESCAPE 'r'
    """
    )

    assert_eq(df, string_table)

    string_table2 = pd.DataFrame({"b": ["a", "b", None, pd.NA, float("nan")]})
    c.create_table("string_table2", string_table2, gpu=gpu)
    df = c.sql(
        """
        SELECT * FROM string_table2
        WHERE b LIKE 'b'
    """
    )

    assert_eq(df, string_table2.iloc[[1]])


def test_null(c):
    df = c.sql(
        """
        SELECT
            c IS NOT NULL AS nn,
            c IS NULL AS n
        FROM user_table_nan
    """
    )

    expected_df = pd.DataFrame(index=[0, 1, 2])
    expected_df["nn"] = [True, False, True]
    expected_df["nn"] = expected_df["nn"].astype("boolean")
    expected_df["n"] = [False, True, False]
    assert_eq(df, expected_df)

    df = c.sql(
        """
        SELECT
            a IS NOT NULL AS nn,
            a IS NULL AS n
        FROM string_table
    """
    )

    expected_df = pd.DataFrame(index=[0, 1, 2, 3])
    expected_df["nn"] = [True, True, True, True]
    expected_df["nn"] = expected_df["nn"].astype("boolean")
    expected_df["n"] = [False, False, False, False]
    assert_eq(df, expected_df)


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_coalesce(c, gpu):
    df = dd.from_pandas(
        pd.DataFrame({"a": [1, 2, 3], "b": [np.nan] * 3}), npartitions=1
    )
    c.create_table("df", df, gpu=gpu)

    df = c.sql(
        """
        SELECT
            COALESCE(3, 5) as c1,
            COALESCE(NULL, NULL) as c2,
            COALESCE(NULL, 'hi') as c3,
            COALESCE(NULL, NULL, 'bye', 5/0) as c4,
            COALESCE(NULL, 3/2, NULL, 'fly') as c5,
            COALESCE(SUM(b), 'why', 2.2) as c6,
            COALESCE(NULL, MEAN(b), MEAN(a), 4/0) as c7
        FROM df
        """
    )

    expected_df = pd.DataFrame(
        {
            "c1": [3],
            "c2": [np.nan],
            "c3": ["hi"],
            "c4": ["bye"],
            "c5": ["1"],
            "c6": ["why"],
            "c7": [2.0],
        }
    )

    assert_eq(df, expected_df, check_dtype=False)

    df = c.sql(
        """
        SELECT
            COALESCE(a, b) as c1,
            COALESCE(b, a) as c2,
            COALESCE(a, a) as c3,
            COALESCE(b, b) as c4
        FROM df
        """
    )

    expected_df = pd.DataFrame(
        {
            "c1": [1, 2, 3],
            "c2": [1, 2, 3],
            "c3": [1, 2, 3],
            "c4": [np.nan] * 3,
        }
    )

    assert_eq(df, expected_df, check_dtype=False)
    c.drop_table("df")


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
    )

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
    assert_eq(df, expected_df)


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
            , MOD(b, 4) AS "mod"
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
    )

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
    expected_df["mod"] = np.mod(df.b, 4)
    expected_df["power"] = np.power(df.b, 2)
    expected_df["power2"] = np.power(df.b, df.a)
    expected_df["radians"] = df.b / 180 * np.pi
    expected_df["round"] = np.round(df.b)
    expected_df["round2"] = np.round(df.b, 3)
    expected_df["sign"] = np.sign(df.b)
    expected_df["sin"] = np.sin(df.b)
    expected_df["tan"] = np.tan(df.b)
    expected_df["truncate"] = np.trunc(df.b)
    assert_eq(result_df, expected_df)


def test_integer_div(c, df_simple):
    df = c.sql(
        """
        SELECT
            1 / a AS a,
            a / 2 AS b,
            1.0 / a AS c
        FROM df_simple
    """
    )

    expected_df = pd.DataFrame(
        {
            "a": (1 // df_simple.a).astype("Int64"),
            "b": (df_simple.a // 2).astype("Int64"),
            "c": 1 / df_simple.a,
        }
    )

    assert_eq(df, expected_df)


@pytest.mark.xfail(reason="Subquery expressions not yet enabled")
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
    )

    assert_eq(df, user_table_2[user_table_2.c.isin(user_table_1.b)], check_index=False)


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_string_functions(c, gpu):
    if gpu:
        input_table = "gpu_string_table"
    else:
        input_table = "string_table"

    df = c.sql(
        f"""
        SELECT
            a || 'hello' || a AS a,
            CONCAT(a, 'hello', a) as b,
            CHAR_LENGTH(a) AS c,
            UPPER(a) AS d,
            LOWER(a) AS e,
            -- POSITION('a' IN a FROM 4) AS f,
            -- POSITION('ZL' IN a) AS g,
            TRIM('a' FROM a) AS h,
            TRIM(BOTH 'a' FROM a) AS i,
            TRIM(LEADING 'a' FROM a) AS j,
            TRIM(TRAILING 'a' FROM a) AS k,
            -- OVERLAY(a PLACING 'XXX' FROM -1) AS l,
            -- OVERLAY(a PLACING 'XXX' FROM 2 FOR 4) AS m,
            -- OVERLAY(a PLACING 'XXX' FROM 2 FOR 1) AS n,
            SUBSTRING(a FROM -1) AS o,
            SUBSTRING(a FROM 10) AS p,
            SUBSTRING(a FROM 2) AS q,
            SUBSTRING(a FROM 2 FOR 2) AS r,
            SUBSTR(a, 3, 6) AS s,
            INITCAP(a) AS t,
            INITCAP(UPPER(a)) AS u,
            INITCAP(LOWER(a)) AS v,
            REPLACE(a, 'r', 'l') as w,
            REPLACE('Another String', 'th', 'b') as x
        FROM
            {input_table}
        """
    )

    if gpu:
        df = df.astype({"c": "int64"})  # , "f": "int64", "g": "int64"})

    expected_df = pd.DataFrame(
        {
            "a": ["a normal stringhelloa normal string"],
            "b": ["a normal stringhelloa normal string"],
            "c": [15],
            "d": ["A NORMAL STRING"],
            "e": ["a normal string"],
            # "f": [7], # position from syntax not supported
            # "g": [0],
            "h": [" normal string"],
            "i": [" normal string"],
            "j": [" normal string"],
            "k": ["a normal string"],
            # "l": ["XXXormal string"], # overlay from syntax not supported by parser
            # "m": ["aXXXmal string"],
            # "n": ["aXXXnormal string"],
            "o": ["a normal string"],
            "p": ["string"],
            "q": [" normal string"],
            "r": [" n"],
            "s": ["normal"],
            "t": ["A Normal String"],
            "u": ["A Normal String"],
            "v": ["A Normal String"],
            "w": ["a nolmal stling"],
            "x": ["Anober String"],
        }
    )

    assert_eq(
        df.head(1),
        expected_df,
    )


@pytest.mark.xfail(reason="POSITION syntax not supported by parser")
@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_string_position(c, gpu):
    if gpu:
        input_table = "gpu_string_table"
    else:
        input_table = "string_table"

    df = c.sql(
        f"""
        SELECT
            POSITION('a' IN a FROM 4) AS f,
            POSITION('ZL' IN a) AS g,
        FROM
            {input_table}
        """
    )

    if gpu:
        df = df.astype({"f": "int64", "g": "int64"})

    expected_df = pd.DataFrame(
        {
            "f": [7],
            "g": [0],
        }
    )

    assert_eq(
        df.head(1),
        expected_df,
    )


@pytest.mark.xfail(reason="OVERLAY syntax not supported by parser")
@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_string_overlay(c, gpu):
    if gpu:
        input_table = "gpu_string_table"
    else:
        input_table = "string_table"

    df = c.sql(
        f"""
        SELECT
            OVERLAY(a PLACING 'XXX' FROM -1) AS l,
            OVERLAY(a PLACING 'XXX' FROM 2 FOR 4) AS m,
            OVERLAY(a PLACING 'XXX' FROM 2 FOR 1) AS n,
        FROM
            {input_table}
        """
    )

    if gpu:
        df = df.astype({"c": "int64"})  # , "f": "int64", "g": "int64"})

    expected_df = pd.DataFrame(
        {
            "l": ["XXXormal string"],
            "m": ["aXXXmal string"],
            "n": ["aXXXnormal string"],
        }
    )

    assert_eq(
        df.head(1),
        expected_df,
    )


def test_date_functions(c):
    date = datetime(2021, 10, 3, 15, 53, 42, 47)

    df = dd.from_pandas(pd.DataFrame({"d": [date]}), npartitions=1)
    c.create_table("df", df)

    df = c.sql(
        """
        SELECT
            EXTRACT(CENTURY FROM d) AS "century",
            EXTRACT(DAY FROM d) AS "day",
            EXTRACT(DECADE FROM d) AS "decade",
            EXTRACT(DOW FROM d) AS "dow",
            EXTRACT(DOY FROM d) AS "doy",
            EXTRACT(HOUR FROM d) AS "hour",
            EXTRACT(MICROSECONDS FROM d) AS "microsecond",
            EXTRACT(MILLENNIUM FROM d) AS "millennium",
            EXTRACT(MILLISECONDS FROM d) AS "millisecond",
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
            TIMESTAMPADD(MICROSECOND, 999*1000, d) as "plus_999_millisec",
            TIMESTAMPADD(MICROSECOND, 999, d) as "plus_999_microsec",
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
    )

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
            "plus_999_millisec": [datetime(2021, 10, 3, 15, 53, 42, 1000 * 999 + 47)],
            "plus_999_microsec": [datetime(2021, 10, 3, 15, 53, 42, 1046)],
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

    assert_eq(df, expected_df, check_dtype=False)

    # test exception handling
    with pytest.raises(NotImplementedError):
        df = c.sql(
            """
            SELECT
                FLOOR(d TO YEAR) as floor_to_year
            FROM df
            """
        )


def test_timestampdiff(c):
    ts_literal1 = datetime(2002, 3, 7, 9, 10, 5, 123)
    ts_literal2 = datetime(2001, 6, 5, 10, 11, 6, 234)
    df = dd.from_pandas(
        pd.DataFrame({"ts_literal1": [ts_literal1], "ts_literal2": [ts_literal2]}),
        npartitions=1,
    )
    c.create_table("df", df)

    query = """
        SELECT timestampdiff(NANOSECOND, ts_literal1, ts_literal2) as res0,
        timestampdiff(MICROSECOND, ts_literal1, ts_literal2) as res1,
        timestampdiff(SECOND, ts_literal1, ts_literal2) as res2,
        timestampdiff(MINUTE, ts_literal1, ts_literal2) as res3,
        timestampdiff(HOUR, ts_literal1, ts_literal2) as res4,
        timestampdiff(DAY, ts_literal1, ts_literal2) as res5,
        timestampdiff(WEEK, ts_literal1, ts_literal2) as res6,
        timestampdiff(MONTH, ts_literal1, ts_literal2) as res7,
        timestampdiff(QUARTER, ts_literal1, ts_literal2) as res8,
        timestampdiff(YEAR, ts_literal1, ts_literal2) as res9
        FROM df
    """
    df = c.sql(query)

    expected_df = pd.DataFrame(
        {
            "res0": [-23756338999889000],
            "res1": [-23756338999889],
            "res2": [-23756338],
            "res3": [-395938],
            "res4": [-6598],
            "res5": [-274],
            "res6": [-39],
            "res7": [-9],
            "res8": [-3],
            "res9": [0],
        }
    )

    assert_eq(df, expected_df, check_dtype=False)

    test = pd.DataFrame(
        {
            "a": [
                datetime(2002, 6, 5, 2, 1, 5, 200),
                datetime(2002, 9, 1),
                datetime(1970, 12, 3),
            ],
            "b": [
                datetime(2002, 6, 7, 1, 0, 2, 100),
                datetime(2003, 6, 5),
                datetime(2038, 6, 5),
            ],
        }
    )
    c.create_table("test", test)

    query = (
        "SELECT timestampdiff(NANOSECOND, a, b) as nanoseconds,"
        "timestampdiff(MICROSECOND, a, b) as microseconds,"
        "timestampdiff(SECOND, a, b) as seconds,"
        "timestampdiff(MINUTE, a, b) as minutes,"
        "timestampdiff(HOUR, a, b) as hours,"
        "timestampdiff(DAY, a, b) as days,"
        "timestampdiff(WEEK, a, b) as weeks,"
        "timestampdiff(MONTH, a, b) as months,"
        "timestampdiff(QUARTER, a, b) as quarters,"
        "timestampdiff(YEAR, a, b) as years"
        " FROM test"
    )
    ddf = c.sql(query)

    expected_df = pd.DataFrame(
        {
            "nanoseconds": [
                169136999900000,
                23932800000000000,
                2130278400000000000,
            ],
            "microseconds": [169136999900, 23932800000000, 2130278400000000],
            "seconds": [169136, 23932800, 2130278400],
            "minutes": [2818, 398880, 35504640],
            "hours": [46, 6648, 591744],
            "days": [1, 277, 24656],
            "weeks": [0, 39, 3522],
            "months": [0, 9, 810],
            "quarters": [0, 3, 270],
            "years": [0, 0, 67],
        }
    )

    assert_eq(ddf, expected_df, check_dtype=False)


@pytest.mark.parametrize(
    "gpu",
    [
        False,
        pytest.param(
            True,
            marks=(
                pytest.mark.gpu,
                pytest.mark.xfail(
                    reason="Failing due to dask-cudf bug https://github.com/rapidsai/cudf/issues/12062"
                ),
            ),
        ),
    ],
)
def test_totimestamp(c, gpu):
    df = pd.DataFrame(
        {
            "a": np.array([1203073300, 1406073600, 2806073600]),
        }
    )
    c.create_table("df", df, gpu=gpu)

    df = c.sql(
        """
        SELECT to_timestamp(a) AS date FROM df
    """
    )
    expected_df = pd.DataFrame(
        {
            "date": [
                datetime(2008, 2, 15, 11, 1, 40),
                datetime(2014, 7, 23),
                datetime(2058, 12, 2, 16, 53, 20),
            ],
        }
    )
    assert_eq(df, expected_df, check_dtype=False)

    df = pd.DataFrame(
        {
            "a": np.array(["1997-02-28 10:30:00", "1997-03-28 10:30:01"]),
        }
    )
    c.create_table("df", df, gpu=gpu)

    df = c.sql(
        """
        SELECT to_timestamp(a) AS date FROM df
    """
    )
    expected_df = pd.DataFrame(
        {
            "date": [
                datetime(1997, 2, 28, 10, 30, 0),
                datetime(1997, 3, 28, 10, 30, 1),
            ],
        }
    )
    assert_eq(df, expected_df, check_dtype=False)

    df = pd.DataFrame(
        {
            "a": np.array(["02/28/1997", "03/28/1997"]),
        }
    )
    c.create_table("df", df, gpu=gpu)

    df = c.sql(
        """
        SELECT to_timestamp(a, "%m/%d/%Y") AS date FROM df
    """
    )
    expected_df = pd.DataFrame(
        {
            "date": [
                datetime(1997, 2, 28, 0, 0, 0),
                datetime(1997, 3, 28, 0, 0, 0),
            ],
        }
    )
    # https://github.com/rapidsai/cudf/issues/12062
    if not gpu:
        assert_eq(df, expected_df, check_dtype=False)

    int_input = 1203073300
    df = c.sql(f"SELECT to_timestamp({int_input}) as date")
    expected_df = pd.DataFrame(
        {
            "date": [
                datetime(2008, 2, 15, 11, 1, 40),
            ],
        }
    )
    assert_eq(df, expected_df, check_dtype=False)

    string_input = "1997-02-28 10:30:00"
    df = c.sql(f"SELECT to_timestamp('{string_input}') as date")
    expected_df = pd.DataFrame(
        {
            "date": [
                datetime(1997, 2, 28, 10, 30, 0),
            ],
        }
    )
    assert_eq(df, expected_df, check_dtype=False)

    string_input = "02/28/1997"
    df = c.sql(f"SELECT to_timestamp('{string_input}', '%m/%d/%Y') as date")
    expected_df = pd.DataFrame(
        {
            "date": [
                datetime(1997, 2, 28, 0, 0, 0),
            ],
        }
    )
    assert_eq(df, expected_df, check_dtype=False)


@pytest.mark.parametrize(
    "gpu",
    [
        False,
        pytest.param(
            True,
            marks=(
                pytest.mark.gpu,
                pytest.mark.skipif(
                    not DASK_CUDF_TODATETIME_SUPPORT,
                    reason="Requires https://github.com/dask/dask/pull/9881",
                ),
            ),
        ),
    ],
)
def test_scalar_timestamps(c, gpu):
    df = pd.DataFrame({"d": [1203073300, 1503073700]})
    c.create_table("df", df, gpu=gpu)

    expected_df = pd.DataFrame(
        {
            "dt": [datetime(2008, 2, 20, 11, 1, 40), datetime(2017, 8, 23, 16, 28, 20)],
        }
    )

    df1 = c.sql("SELECT to_timestamp(d) + INTERVAL '5 days' AS dt FROM df")
    assert_eq(df1, expected_df)
    df2 = c.sql("SELECT CAST(d AS TIMESTAMP) + INTERVAL '5 days' AS dt FROM df")
    assert_eq(df2, expected_df)

    df1 = c.sql("SELECT TIMESTAMPADD(DAY, 5, to_timestamp(d)) AS dt FROM df")
    assert_eq(df1, expected_df)
    df2 = c.sql("SELECT TIMESTAMPADD(DAY, 5, d) AS dt FROM df")
    assert_eq(df2, expected_df)
    df3 = c.sql("SELECT TIMESTAMPADD(DAY, 5, CAST(d AS TIMESTAMP)) AS dt FROM df")
    assert_eq(df3, expected_df)

    expected_df = pd.DataFrame({"day": [15, 18]})
    df1 = c.sql("SELECT EXTRACT(DAY FROM to_timestamp(d)) AS day FROM df")
    assert_eq(df1, expected_df, check_dtype=False)
    df2 = c.sql("SELECT EXTRACT(DAY FROM CAST(d AS TIMESTAMP)) AS day FROM df")
    assert_eq(df2, expected_df, check_dtype=False)

    expected_df = pd.DataFrame(
        {
            "ceil_to_day": [datetime(2008, 2, 16), datetime(2017, 8, 19)],
        }
    )
    df1 = c.sql("SELECT CEIL(to_timestamp(d) TO DAY) AS ceil_to_day FROM df")
    assert_eq(df1, expected_df)
    df2 = c.sql("SELECT CEIL(CAST(d AS TIMESTAMP) TO DAY) AS ceil_to_day FROM df")
    assert_eq(df2, expected_df)

    expected_df = pd.DataFrame(
        {
            "floor_to_day": [datetime(2008, 2, 15), datetime(2017, 8, 18)],
        }
    )
    df1 = c.sql("SELECT FLOOR(to_timestamp(d) TO DAY) AS floor_to_day FROM df")
    assert_eq(df1, expected_df)
    df2 = c.sql("SELECT FLOOR(CAST(d AS TIMESTAMP) TO DAY) AS floor_to_day FROM df")
    assert_eq(df2, expected_df)

    df = pd.DataFrame({"d1": [1203073300], "d2": [1503073700]})
    c.create_table("df", df, gpu=gpu)
    expected_df = pd.DataFrame({"dt": [3472]})
    df1 = c.sql(
        "SELECT TIMESTAMPDIFF(DAY, to_timestamp(d1), to_timestamp(d2)) AS dt FROM df"
    )
    assert_eq(df1, expected_df)
    df2 = c.sql("SELECT TIMESTAMPDIFF(DAY, d1, d2) AS dt FROM df")
    assert_eq(df2, expected_df, check_dtype=False)
    df3 = c.sql(
        "SELECT TIMESTAMPDIFF(DAY, CAST(d1 AS TIMESTAMP), CAST(d2 AS TIMESTAMP)) AS dt FROM df"
    )
    assert_eq(df3, expected_df)

    scalar1 = 1203073300
    scalar2 = 1503073700

    expected_df = pd.DataFrame({"dt": [datetime(2008, 2, 20, 11, 1, 40)]})

    df1 = c.sql(f"SELECT to_timestamp({scalar1}) + INTERVAL '5 days' AS dt")
    assert_eq(df1, expected_df)
    # TODO: Fix seconds/nanoseconds conversion
    # df2 = c.sql(f"SELECT CAST({scalar1} AS TIMESTAMP) + INTERVAL '5 days' AS dt")
    # assert_eq(df2, expected_df)

    df1 = c.sql(f"SELECT TIMESTAMPADD(DAY, 5, to_timestamp({scalar1})) AS dt")
    assert_eq(df1, expected_df)
    df2 = c.sql(f"SELECT TIMESTAMPADD(DAY, 5, {scalar1}) AS dt")
    assert_eq(df2, expected_df)
    df3 = c.sql(f"SELECT TIMESTAMPADD(DAY, 5, CAST({scalar1} AS TIMESTAMP)) AS dt")
    assert_eq(df3, expected_df)

    expected_df = pd.DataFrame({"day": [15]})
    df1 = c.sql(f"SELECT EXTRACT(DAY FROM to_timestamp({scalar1})) AS day")
    assert_eq(df1, expected_df, check_dtype=False)
    # TODO: Fix seconds/nanoseconds conversion
    # df2 = c.sql(f"SELECT EXTRACT(DAY FROM CAST({scalar1} AS TIMESTAMP)) AS day")
    # assert_eq(df2, expected_df, check_dtype=False)

    expected_df = pd.DataFrame({"ceil_to_day": [datetime(2008, 2, 16)]})
    df1 = c.sql(f"SELECT CEIL(to_timestamp({scalar1}) TO DAY) AS ceil_to_day")
    assert_eq(df1, expected_df)
    df2 = c.sql(f"SELECT CEIL(CAST({scalar1} AS TIMESTAMP) TO DAY) AS ceil_to_day")
    assert_eq(df2, expected_df)

    expected_df = pd.DataFrame({"floor_to_day": [datetime(2008, 2, 15)]})
    df1 = c.sql(f"SELECT FLOOR(to_timestamp({scalar1}) TO DAY) AS floor_to_day")
    assert_eq(df1, expected_df)
    df2 = c.sql(f"SELECT FLOOR(CAST({scalar1} AS TIMESTAMP) TO DAY) AS floor_to_day")
    assert_eq(df2, expected_df)

    expected_df = pd.DataFrame({"dt": [3472]})
    df1 = c.sql(
        f"SELECT TIMESTAMPDIFF(DAY, to_timestamp({scalar1}), to_timestamp({scalar2})) AS dt"
    )
    assert_eq(df1, expected_df)
    df2 = c.sql(f"SELECT TIMESTAMPDIFF(DAY, {scalar1}, {scalar2}) AS dt")
    assert_eq(df2, expected_df, check_dtype=False)
    df3 = c.sql(
        f"SELECT TIMESTAMPDIFF(DAY, CAST({scalar1} AS TIMESTAMP), CAST({scalar2} AS TIMESTAMP)) AS dt"
    )
    assert_eq(df3, expected_df)
