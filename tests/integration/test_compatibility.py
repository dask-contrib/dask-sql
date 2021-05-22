"""
The tests in this module are taken from
the fugue-sql module to test the compatibility
with their "understanding" of SQL
They run randomized tests and compare with sqlite.

There are some changes compared to the fugueSQL
tests, especially when it comes to sort order:
dask-sql does not enforce a specific order after groupby
"""

import sqlite3
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from dask_sql import Context


def eq_sqlite(sql, **dfs):
    c = Context()
    engine = sqlite3.connect(":memory:")

    for name, df in dfs.items():
        c.create_table(name, df)
        df.to_sql(name, engine, index=False)

    dask_result = c.sql(sql).compute().reset_index(drop=True)
    sqlite_result = pd.read_sql(sql, engine).reset_index(drop=True)

    # Make sure SQL and Dask use the same "NULL" value
    dask_result = dask_result.fillna(np.NaN)
    sqlite_result = sqlite_result.fillna(np.NaN)

    assert_frame_equal(dask_result, sqlite_result, check_dtype=False)


def make_rand_df(size: int, **kwargs):
    np.random.seed(0)
    data = {}
    for k, v in kwargs.items():
        if not isinstance(v, tuple):
            v = (v, 0.0)
        dt, null_ct = v[0], v[1]
        if dt is int:
            s = np.random.randint(10, size=size)
        elif dt is bool:
            s = np.where(np.random.randint(2, size=size), True, False)
        elif dt is float:
            s = np.random.rand(size)
        elif dt is str:
            r = [f"ssssss{x}" for x in range(10)]
            c = np.random.randint(10, size=size)
            s = np.array([r[x] for x in c])
        elif dt is datetime:
            rt = [datetime(2020, 1, 1) + timedelta(days=x) for x in range(10)]
            c = np.random.randint(10, size=size)
            s = np.array([rt[x] for x in c])
        else:
            raise NotImplementedError
        ps = pd.Series(s)
        if null_ct > 0:
            idx = np.random.choice(size, null_ct, replace=False).tolist()
            ps[idx] = None
        data[k] = ps
    return pd.DataFrame(data)


def test_basic_select_from():
    df = make_rand_df(5, a=(int, 2), b=(str, 3), c=(float, 4))
    eq_sqlite("SELECT 1 AS a, 1.5 AS b, 'x' AS c")
    eq_sqlite("SELECT 1+2 AS a, 1.5*3 AS b, 'x' AS c")
    eq_sqlite("SELECT * FROM a", a=df)
    eq_sqlite("SELECT * FROM a AS x", a=df)
    eq_sqlite("SELECT b AS bb, a+1-2*3.0/4 AS cc, x.* FROM a AS x", a=df)
    eq_sqlite("SELECT *, 1 AS x, 2.5 AS y, 'z' AS z FROM a AS x", a=df)
    eq_sqlite("SELECT *, -(1.0+a)/3 AS x, +(2.5) AS y FROM a AS x", a=df)


def test_case_when():
    a = make_rand_df(100, a=(int, 20), b=(str, 30), c=(float, 40))
    eq_sqlite(
        """
        SELECT a,b,c,
            CASE
                WHEN a<10 THEN a+3
                WHEN c<0.5 THEN a+5
                ELSE (1+2)*3 + a
            END AS d
        FROM a
        """,
        a=a,
    )


def test_drop_duplicates():
    # simplest
    a = make_rand_df(100, a=int, b=int)
    eq_sqlite(
        """
        SELECT DISTINCT b, a FROM a
        ORDER BY a NULLS LAST, b NULLS FIRST
        """,
        a=a,
    )
    # mix of number and nan
    a = make_rand_df(100, a=(int, 50), b=(int, 50))
    eq_sqlite(
        """
        SELECT DISTINCT b, a FROM a
        ORDER BY a NULLS LAST, b NULLS FIRST
        """,
        a=a,
    )
    # mix of number and string and nulls
    a = make_rand_df(100, a=(int, 50), b=(str, 50), c=float)
    eq_sqlite(
        """
        SELECT DISTINCT b, a FROM a
        ORDER BY a NULLS LAST, b NULLS FIRST
        """,
        a=a,
    )


def test_order_by_no_limit():
    a = make_rand_df(100, a=(int, 50), b=(str, 50), c=float)
    eq_sqlite(
        """
        SELECT DISTINCT b, a FROM a
        ORDER BY a NULLS LAST, b NULLS FIRST
        """,
        a=a,
    )


def test_order_by_limit():
    a = make_rand_df(100, a=(int, 50), b=(str, 50), c=float)
    eq_sqlite(
        """
        SELECT DISTINCT b, a FROM a LIMIT 0
        """,
        a=a,
    )
    eq_sqlite(
        """
        SELECT DISTINCT b, a FROM a ORDER BY a NULLS FIRST, b NULLS FIRST LIMIT 2
        """,
        a=a,
    )
    eq_sqlite(
        """
        SELECT b, a FROM a
            ORDER BY a NULLS LAST, b NULLS FIRST LIMIT 10
        """,
        a=a,
    )


def test_where():
    df = make_rand_df(100, a=(int, 30), b=(str, 30), c=(float, 30))
    eq_sqlite("SELECT * FROM a WHERE TRUE OR TRUE", a=df)
    eq_sqlite("SELECT * FROM a WHERE TRUE AND TRUE", a=df)
    eq_sqlite("SELECT * FROM a WHERE FALSE OR FALSE", a=df)
    eq_sqlite("SELECT * FROM a WHERE FALSE AND FALSE", a=df)

    eq_sqlite("SELECT * FROM a WHERE TRUE OR b<='ssssss8'", a=df)
    eq_sqlite("SELECT * FROM a WHERE TRUE AND b<='ssssss8'", a=df)
    eq_sqlite("SELECT * FROM a WHERE FALSE OR b<='ssssss8'", a=df)
    eq_sqlite("SELECT * FROM a WHERE FALSE AND b<='ssssss8'", a=df)
    eq_sqlite("SELECT * FROM a WHERE a=10 OR b<='ssssss8'", a=df)
    eq_sqlite("SELECT * FROM a WHERE c IS NOT NULL OR (a<5 AND b IS NOT NULL)", a=df)

    df = make_rand_df(100, a=(float, 30), b=(float, 30), c=(float, 30))
    eq_sqlite("SELECT * FROM a WHERE a<0.5 AND b<0.5 AND c<0.5", a=df)
    eq_sqlite("SELECT * FROM a WHERE a<0.5 OR b<0.5 AND c<0.5", a=df)
    eq_sqlite("SELECT * FROM a WHERE a IS NULL OR (b<0.5 AND c<0.5)", a=df)
    eq_sqlite("SELECT * FROM a WHERE a*b IS NULL OR (b*c<0.5 AND c*a<0.5)", a=df)


def test_in_between():
    df = make_rand_df(10, a=(int, 3), b=(str, 3))
    eq_sqlite("SELECT * FROM a WHERE a IN (2,4,6)", a=df)
    eq_sqlite("SELECT * FROM a WHERE a BETWEEN 2 AND 4+1", a=df)
    eq_sqlite("SELECT * FROM a WHERE a NOT IN (2,4,6) AND a IS NOT NULL", a=df)
    eq_sqlite("SELECT * FROM a WHERE a NOT BETWEEN 2 AND 4+1 AND a IS NOT NULL", a=df)


def test_join_inner():
    a = make_rand_df(100, a=(int, 40), b=(str, 40), c=(float, 40))
    b = make_rand_df(80, d=(float, 10), a=(int, 10), b=(str, 10))
    eq_sqlite(
        """
        SELECT
        a.*, d, d*c AS x
        FROM a
        INNER JOIN b ON a.a=b.a AND a.b=b.b
        ORDER BY a.a NULLS FIRST, a.b NULLS FIRST, a.c NULLS FIRST, d NULLS FIRST
        """,
        a=a,
        b=b,
    )


def test_join_left():
    a = make_rand_df(100, a=(int, 40), b=(str, 40), c=(float, 40))
    b = make_rand_df(80, d=(float, 10), a=(int, 10), b=(str, 10))
    eq_sqlite(
        """
        SELECT
        a.*, d, d*c AS x
        FROM a LEFT JOIN b ON a.a=b.a AND a.b=b.b
        ORDER BY a.a NULLS FIRST, a.b NULLS FIRST, a.c NULLS FIRST, d NULLS FIRST
        """,
        a=a,
        b=b,
    )


def test_join_cross():
    a = make_rand_df(10, a=(int, 4), b=(str, 4), c=(float, 4))
    b = make_rand_df(20, dd=(float, 1), aa=(int, 1), bb=(str, 1))
    eq_sqlite("SELECT * FROM a CROSS JOIN b", a=a, b=b)


def test_join_multi():
    a = make_rand_df(100, a=(int, 40), b=(str, 40), c=(float, 40))
    b = make_rand_df(80, d=(float, 10), a=(int, 10), b=(str, 10))
    c = make_rand_df(80, dd=(float, 10), a=(int, 10), b=(str, 10))
    eq_sqlite(
        """
        SELECT a.*,d,dd FROM a
            INNER JOIN b ON a.a=b.a AND a.b=b.b
            INNER JOIN c ON a.a=c.a AND c.b=b.b
        ORDER BY a.a NULLS FIRST, a.b NULLS FIRST, a.c NULLS FIRST, dd NULLS FIRST, d NULLS FIRST
        """,
        a=a,
        b=b,
        c=c,
    )


def test_agg_count_no_group_by():
    a = make_rand_df(
        100, a=(int, 50), b=(str, 50), c=(int, 30), d=(str, 40), e=(float, 40)
    )
    eq_sqlite(
        """
        SELECT
            COUNT(a) AS c_a,
            COUNT(DISTINCT a) AS cd_a,
            COUNT(b) AS c_b,
            COUNT(DISTINCT b) AS cd_b,
            COUNT(c) AS c_c,
            COUNT(DISTINCT c) AS cd_c,
            COUNT(d) AS c_d,
            COUNT(DISTINCT d) AS cd_d,
            COUNT(e) AS c_e,
            COUNT(DISTINCT a) AS cd_e
        FROM a
        """,
        a=a,
    )


def test_agg_count():
    a = make_rand_df(
        100, a=(int, 50), b=(str, 50), c=(int, 30), d=(str, 40), e=(float, 40)
    )
    eq_sqlite(
        """
        SELECT
            a, b, a+1 AS c,
            COUNT(c) AS c_c,
            COUNT(DISTINCT c) AS cd_c,
            COUNT(d) AS c_d,
            COUNT(DISTINCT d) AS cd_d,
            COUNT(e) AS c_e,
            COUNT(DISTINCT a) AS cd_e
        FROM a GROUP BY a, b
        """,
        a=a,
    )


def test_agg_sum_avg_no_group_by():
    eq_sqlite(
        """
        SELECT
            SUM(a) AS sum_a,
            AVG(a) AS avg_a
        FROM a
        """,
        a=pd.DataFrame({"a": [float("nan")]}),
    )
    a = make_rand_df(
        100, a=(int, 50), b=(str, 50), c=(int, 30), d=(str, 40), e=(float, 40)
    )
    eq_sqlite(
        """
        SELECT
            SUM(a) AS sum_a,
            AVG(a) AS avg_a,
            SUM(c) AS sum_c,
            AVG(c) AS avg_c,
            SUM(e) AS sum_e,
            AVG(e) AS avg_e,
            SUM(a)+AVG(e) AS mix_1,
            SUM(a+e) AS mix_2
        FROM a
        """,
        a=a,
    )


def test_agg_sum_avg():
    a = make_rand_df(
        100, a=(int, 50), b=(str, 50), c=(int, 30), d=(str, 40), e=(float, 40)
    )
    eq_sqlite(
        """
        SELECT
            a,b, a+1 AS c,
            SUM(c) AS sum_c,
            AVG(c) AS avg_c,
            SUM(e) AS sum_e,
            AVG(e) AS avg_e,
            SUM(a)+AVG(e) AS mix_1,
            SUM(a+e) AS mix_2
        FROM a GROUP BY a,b
        """,
        a=a,
    )


def test_agg_min_max_no_group_by():
    a = make_rand_df(
        100, a=(int, 50), b=(str, 50), c=(int, 30), d=(str, 40), e=(float, 40)
    )
    eq_sqlite(
        """
        SELECT
            MIN(a) AS min_a,
            MAX(a) AS max_a,
            MIN(b) AS min_b,
            MAX(b) AS max_b,
            MIN(c) AS min_c,
            MAX(c) AS max_c,
            MIN(d) AS min_d,
            MAX(d) AS max_d,
            MIN(e) AS min_e,
            MAX(e) AS max_e,
            MIN(a+e) AS mix_1,
            MIN(a)+MIN(e) AS mix_2
        FROM a
        """,
        a=a,
    )


def test_agg_min_max():
    a = make_rand_df(
        100, a=(int, 50), b=(str, 50), c=(int, 30), d=(str, 40), e=(float, 40)
    )
    eq_sqlite(
        """
        SELECT
            a, b, a+1 AS c,
            MIN(c) AS min_c,
            MAX(c) AS max_c,
            MIN(d) AS min_d,
            MAX(d) AS max_d,
            MIN(e) AS min_e,
            MAX(e) AS max_e,
            MIN(a+e) AS mix_1,
            MIN(a)+MIN(e) AS mix_2
        FROM a GROUP BY a, b
        """,
        a=a,
    )


def test_window_row_number():
    a = make_rand_df(10, a=int, b=(float, 5))
    eq_sqlite(
        """
        SELECT *,
            ROW_NUMBER() OVER (ORDER BY a ASC, b DESC NULLS FIRST) AS a1,
            ROW_NUMBER() OVER (ORDER BY a ASC, b DESC NULLS LAST) AS a2,
            ROW_NUMBER() OVER (ORDER BY a ASC, b ASC NULLS FIRST) AS a3,
            ROW_NUMBER() OVER (ORDER BY a ASC, b ASC NULLS LAST) AS a4,
            ROW_NUMBER() OVER (PARTITION BY a ORDER BY a,b DESC NULLS FIRST) AS a5
        FROM a
        ORDER BY a, b NULLS FIRST
        """,
        a=a,
    )

    a = make_rand_df(100, a=(int, 50), b=(str, 50), c=(int, 30), d=(str, 40), e=float)
    eq_sqlite(
        """
        SELECT *,
            ROW_NUMBER() OVER (ORDER BY a ASC NULLS LAST, b DESC NULLS FIRST, e) AS a1,
            ROW_NUMBER() OVER (ORDER BY a ASC NULLS FIRST, b DESC NULLS LAST, e) AS a2,
            ROW_NUMBER() OVER (PARTITION BY a ORDER BY a NULLS FIRST, b DESC NULLS LAST, e) AS a3,
            ROW_NUMBER() OVER (PARTITION BY a,c ORDER BY a NULLS FIRST, b DESC NULLS LAST, e) AS a4
        FROM a
        ORDER BY a NULLS FIRST, b NULLS FIRST, c NULLS FIRST, d NULLS FIRST, e
        """,
        a=a,
    )


def test_window_row_number_partition_by():
    a = make_rand_df(100, a=int, b=(float, 50))
    eq_sqlite(
        """
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY a ORDER BY a, b DESC NULLS FIRST) AS a5
        FROM a
        ORDER BY a, b NULLS FIRST, a5
        """,
        a=a,
    )

    a = make_rand_df(100, a=(int, 50), b=(str, 50), c=(int, 30), d=(str, 40), e=float)
    eq_sqlite(
        """
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY a ORDER BY a NULLS FIRST, b DESC NULLS FIRST, e) AS a3,
            ROW_NUMBER() OVER (PARTITION BY a,c ORDER BY a NULLS FIRST, b DESC NULLS FIRST, e) AS a4
        FROM a
        ORDER BY a NULLS FIRST, b NULLS FIRST, c NULLS FIRST, d NULLS FIRST, e
        """,
        a=a,
    )


# TODO: Except not implemented so far
# def test_window_ranks():
#     a = make_rand_df(100, a=int, b=(float, 50), c=(str, 50))
#     eq_sqlite(
#         """
#         SELECT *,
#             RANK() OVER (PARTITION BY a ORDER BY b DESC NULLS FIRST, c) AS a1,
#             DENSE_RANK() OVER (ORDER BY a ASC, b DESC NULLS LAST, c DESC) AS a2,
#             PERCENT_RANK() OVER (ORDER BY a ASC, b ASC NULLS LAST, c) AS a4
#         FROM a
#         """,
#         a=a,
#     )

# TODO: Except not implemented so far
# def test_window_ranks_partition_by():
#     a = make_rand_df(100, a=int, b=(float, 50), c=(str, 50))
#     eq_sqlite(
#         """
#         SELECT *,
#             RANK() OVER (PARTITION BY a ORDER BY b DESC NULLS FIRST, c) AS a1,
#             DENSE_RANK() OVER
#                 (PARTITION BY a ORDER BY a ASC, b DESC NULLS LAST, c DESC)
#                 AS a2,
#             PERCENT_RANK() OVER
#                 (PARTITION BY a ORDER BY a ASC, b ASC NULLS LAST, c) AS a4
#         FROM a
#         """,
#         a=a,
#     )

# TODO: Except not implemented so far
# def test_window_lead_lag():
#     a = make_rand_df(100, a=float, b=(int, 50), c=(str, 50))
#     eq_sqlite(
#         """
#         SELECT
#             LEAD(b,1) OVER (ORDER BY a) AS a1,
#             LEAD(b,2,10) OVER (ORDER BY a) AS a2,
#             LEAD(b,1) OVER (PARTITION BY c ORDER BY a) AS a3,
#             LEAD(b,1) OVER (PARTITION BY c ORDER BY b, a ASC NULLS LAST) AS a5,

#             LAG(b,1) OVER (ORDER BY a) AS b1,
#             LAG(b,2,10) OVER (ORDER BY a) AS b2,
#             LAG(b,1) OVER (PARTITION BY c ORDER BY a) AS b3,
#             LAG(b,1) OVER (PARTITION BY c ORDER BY b, a ASC NULLS LAST) AS b5
#         FROM a
#         """,
#         a=a,
#     )

# TODO: Except not implemented so far
# def test_window_lead_lag_partition_by():
#     a = make_rand_df(100, a=float, b=(int, 50), c=(str, 50))
#     eq_sqlite(
#         """
#         SELECT
#             LEAD(b,1,10) OVER (PARTITION BY c ORDER BY a) AS a3,
#             LEAD(b,1) OVER (PARTITION BY c ORDER BY b, a ASC NULLS LAST) AS a5,

#             LAG(b,1) OVER (PARTITION BY c ORDER BY a) AS b3,
#             LAG(b,1) OVER (PARTITION BY c ORDER BY b, a ASC NULLS LAST) AS b5
#         FROM a
#         """,
#         a=a,
#     )


def test_window_sum_avg():
    a = make_rand_df(100, a=float, b=(int, 50), c=(str, 50))
    for func in ["SUM", "AVG"]:
        eq_sqlite(
            f"""
            SELECT a,b,
                {func}(b) OVER () AS a1,
                {func}(b) OVER (PARTITION BY c) AS a2,
                {func}(b+a) OVER (PARTITION BY c,b) AS a3,
                {func}(b+a) OVER (PARTITION BY b ORDER BY a NULLS FIRST
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS a4,
                {func}(b+a) OVER (PARTITION BY b ORDER BY a DESC NULLS FIRST
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS a5,
                {func}(b+a) OVER (PARTITION BY b ORDER BY a NULLS FIRST
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                    AS a6
            FROM a
            ORDER BY a NULLS FIRST, b NULLS FIRST, c NULLS FIRST
            """,
            a=a,
        )
        # irregular windows
        eq_sqlite(
            f"""
            SELECT a,b,
                {func}(b) OVER (PARTITION BY b ORDER BY a DESC NULLS FIRST
                    ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS a6,
                {func}(b) OVER (PARTITION BY b ORDER BY a DESC NULLS FIRST
                    ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) AS a7,
                {func}(b) OVER (PARTITION BY b ORDER BY a DESC NULLS FIRST
                    ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) AS a8
            FROM a
            ORDER BY a NULLS FIRST, b NULLS FIRST, c NULLS FIRST
            """,
            a=a,
        )


def test_window_sum_avg_partition_by():
    a = make_rand_df(100, a=float, b=(int, 50), c=(str, 50))
    for func in ["SUM", "AVG"]:
        eq_sqlite(
            f"""
            SELECT a,b,
                {func}(b+a) OVER (PARTITION BY c,b) AS a3,
                {func}(b+a) OVER (PARTITION BY b ORDER BY a NULLS FIRST
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS a4,
                {func}(b+a) OVER (PARTITION BY b ORDER BY a DESC NULLS FIRST
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS a5,
                {func}(b+a) OVER (PARTITION BY b ORDER BY a NULLS FIRST
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                    AS a6
            FROM a
            ORDER BY a NULLS FIRST, b NULLS FIRST, c NULLS FIRST
            """,
            a=a,
        )
        # irregular windows
        eq_sqlite(
            f"""
            SELECT a,b,
                {func}(b) OVER (PARTITION BY b ORDER BY a DESC NULLS FIRST
                    ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS a6,
                {func}(b) OVER (PARTITION BY b ORDER BY a DESC NULLS FIRST
                    ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) AS a7,
                {func}(b) OVER (PARTITION BY b ORDER BY a DESC NULLS FIRST
                    ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) AS a8
            FROM a
            ORDER BY a NULLS FIRST, b NULLS FIRST, c NULLS FIRST
            """,
            a=a,
        )


def test_window_min_max():
    for func in ["MIN", "MAX"]:
        a = make_rand_df(100, a=float, b=(int, 50), c=(str, 50))
        eq_sqlite(
            f"""
            SELECT a,b,
                {func}(b) OVER () AS a1,
                {func}(b) OVER (PARTITION BY c) AS a2,
                {func}(b+a) OVER (PARTITION BY c,b) AS a3,
                {func}(b+a) OVER (PARTITION BY b ORDER BY a
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS a4,
                {func}(b+a) OVER (PARTITION BY b ORDER BY a DESC
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS a5,
                {func}(b+a) OVER (PARTITION BY b ORDER BY a
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                    AS a6
            FROM a
            ORDER BY a NULLS FIRST, b NULLS FIRST, c NULLS FIRST
            """,
            a=a,
        )
        # irregular windows
        eq_sqlite(
            f"""
            SELECT a,b,
                {func}(b) OVER (ORDER BY a DESC
                    ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS a6,
                {func}(b) OVER (ORDER BY a DESC
                    ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) AS a7,
                {func}(b) OVER (ORDER BY a DESC
                    ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) AS a8
            FROM a
            ORDER BY a NULLS FIRST, b NULLS FIRST, c NULLS FIRST
            """,
            a=a,
        )
        b = make_rand_df(10, a=float, b=(int, 0), c=(str, 0))
        eq_sqlite(
            f"""
            SELECT a,b,
                {func}(b) OVER (PARTITION BY b ORDER BY a DESC
                    ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS a6
            FROM a
            ORDER BY a NULLS FIRST, b NULLS FIRST, c NULLS FIRST
            """,
            a=b,
        )


def test_window_min_max_partition_by():
    for func in ["MIN", "MAX"]:
        a = make_rand_df(100, a=float, b=(int, 50), c=(str, 50))
        eq_sqlite(
            f"""
            SELECT a,b,
                {func}(b) OVER (PARTITION BY c) AS a2,
                {func}(b+a) OVER (PARTITION BY c,b) AS a3,
                {func}(b+a) OVER (PARTITION BY b ORDER BY a
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS a4,
                {func}(b+a) OVER (PARTITION BY b ORDER BY a DESC
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS a5,
                {func}(b+a) OVER (PARTITION BY b ORDER BY a
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                    AS a6
            FROM a
            ORDER BY a NULLS FIRST, b NULLS FIRST, c NULLS FIRST
            """,
            a=a,
        )
        b = make_rand_df(10, a=float, b=(int, 0), c=(str, 0))
        eq_sqlite(
            f"""
            SELECT a,b,
                {func}(b) OVER (PARTITION BY b ORDER BY a DESC
                    ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS a6
            FROM a
            ORDER BY a NULLS FIRST, b NULLS FIRST, c NULLS FIRST
            """,
            a=b,
        )


def test_window_count():
    for func in ["COUNT"]:
        a = make_rand_df(100, a=float, b=(int, 50), c=(str, 50))
        eq_sqlite(
            f"""
            SELECT a,b,
                {func}(b) OVER () AS a1,
                {func}(b) OVER (PARTITION BY c) AS a2,
                {func}(b+a) OVER (PARTITION BY c,b) AS a3,
                {func}(b+a) OVER (PARTITION BY b ORDER BY a
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS a4,
                {func}(b+a) OVER (PARTITION BY b ORDER BY a DESC
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS a5,
                {func}(b+a) OVER (PARTITION BY b ORDER BY a
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                    AS a6

                -- No support for rolling on string types
                -- {func}(c) OVER () AS b1,
                -- {func}(c) OVER (PARTITION BY c) AS b2,
                -- {func}(c) OVER (PARTITION BY c,b) AS b3,
                -- {func}(c) OVER (PARTITION BY b ORDER BY a
                --     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS b4,
                -- {func}(c) OVER (PARTITION BY b ORDER BY a DESC
                --     ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS b5,
                -- {func}(c) OVER (PARTITION BY b ORDER BY a
                --     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                --     AS b6
            FROM a
            ORDER BY a NULLS FIRST, b NULLS FIRST, c NULLS FIRST
            """,
            a=a,
        )
        # irregular windows
        eq_sqlite(
            f"""
            SELECT a,b,
                {func}(b) OVER (ORDER BY a DESC
                    ROWS BETWEEN 2 PRECEDING AND 0 PRECEDING) AS a6,
                {func}(b) OVER (PARTITION BY c ORDER BY a DESC
                    ROWS BETWEEN 2 PRECEDING AND 0 PRECEDING) AS a9 --,

                -- No support for rolling on string types
                -- {func}(c) OVER (ORDER BY a DESC
                --     ROWS BETWEEN 2 PRECEDING AND 0 PRECEDING) AS b6,
                -- {func}(c) OVER (PARTITION BY c ORDER BY a DESC
                --     ROWS BETWEEN 2 PRECEDING AND 0 PRECEDING) AS b9
            FROM a
            ORDER BY a NULLS FIRST, b NULLS FIRST, c NULLS FIRST
            """,
            a=a,
        )


def test_window_count_partition_by():
    for func in ["COUNT"]:
        a = make_rand_df(100, a=float, b=(int, 50), c=(str, 50))
        eq_sqlite(
            f"""
            SELECT a,b,
                {func}(b) OVER (PARTITION BY c) AS a2,
                {func}(b+a) OVER (PARTITION BY c,b) AS a3,
                {func}(b+a) OVER (PARTITION BY b ORDER BY a
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS a4,
                {func}(b+a) OVER (PARTITION BY b ORDER BY a DESC
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS a5,
                {func}(b+a) OVER (PARTITION BY b ORDER BY a
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                    AS a6 --,

                -- No support for rolling on string types
                -- {func}(c) OVER (PARTITION BY c) AS b2,
                -- {func}(c) OVER (PARTITION BY c,b) AS b3,
                -- {func}(c) OVER (PARTITION BY b ORDER BY a
                --     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS b4,
                -- {func}(c) OVER (PARTITION BY b ORDER BY a DESC
                --     ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS b5,
                -- {func}(c) OVER (PARTITION BY b ORDER BY a
                --     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                --     AS b6
            FROM a
            ORDER BY a NULLS FIRST, b NULLS FIRST, c NULLS FIRST
            """,
            a=a,
        )
        # irregular windows
        eq_sqlite(
            f"""
            SELECT a,b,
                {func}(b) OVER (PARTITION BY c ORDER BY a DESC
                    ROWS BETWEEN 2 PRECEDING AND 0 PRECEDING) AS a9 --,

                -- No support for rolling on string types
                -- {func}(c) OVER (PARTITION BY c ORDER BY a DESC
                --     ROWS BETWEEN 2 PRECEDING AND 0 PRECEDING) AS b9
            FROM a
            ORDER BY a NULLS FIRST, b NULLS FIRST, c NULLS FIRST
            """,
            a=a,
        )


def test_nested_query():
    a = make_rand_df(100, a=float, b=(int, 50), c=(str, 50))
    eq_sqlite(
        """
        SELECT * FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY c ORDER BY b NULLS FIRST, a ASC NULLS LAST) AS r
        FROM a)
        WHERE r=1
        ORDER BY a NULLS LAST, b NULLS LAST, c NULLS LAST
        """,
        a=a,
    )


def test_union():
    a = make_rand_df(30, b=(int, 10), c=(str, 10))
    b = make_rand_df(80, b=(int, 50), c=(str, 50))
    c = make_rand_df(100, b=(int, 50), c=(str, 50))
    eq_sqlite(
        """
        SELECT * FROM a
            UNION SELECT * FROM b
            UNION SELECT * FROM c
        ORDER BY b NULLS FIRST, c NULLS FIRST
        """,
        a=a,
        b=b,
        c=c,
    )
    eq_sqlite(
        """
        SELECT * FROM a
            UNION ALL SELECT * FROM b
            UNION ALL SELECT * FROM c
        ORDER BY b NULLS FIRST, c NULLS FIRST
        """,
        a=a,
        b=b,
        c=c,
    )


# TODO: Except not implemented so far
# def test_except():
#     a = make_rand_df(30, b=(int, 10), c=(str, 10))
#     b = make_rand_df(80, b=(int, 50), c=(str, 50))
#     c = make_rand_df(100, b=(int, 50), c=(str, 50))
#     eq_sqlite(
#         """
#         SELECT * FROM c
#             EXCEPT SELECT * FROM b
#             EXCEPT SELECT * FROM c
#         """,
#         a=a,
#         b=b,
#         c=c,
#     )

# TODO: Intersect not implemented so far
# def test_intersect():
#     a = make_rand_df(30, b=(int, 10), c=(str, 10))
#     b = make_rand_df(80, b=(int, 50), c=(str, 50))
#     c = make_rand_df(100, b=(int, 50), c=(str, 50))
#     eq_sqlite(
#         """
#         SELECT * FROM c
#             INTERSECT SELECT * FROM b
#             INTERSECT SELECT * FROM c
#         """,
#         a=a,
#         b=b,
#         c=c,
#     )


def test_with():
    a = make_rand_df(30, a=(int, 10), b=(str, 10))
    b = make_rand_df(80, ax=(int, 10), bx=(str, 10))
    eq_sqlite(
        """
        WITH
            aa AS (
                SELECT a AS aa, b AS bb FROM a
            ),
            c AS (
                SELECT aa-1 AS aa, bb FROM aa
            )
        SELECT * FROM c UNION SELECT * FROM b
        ORDER BY aa NULLS FIRST, bb NULLS FIRST
        """,
        a=a,
        b=b,
    )


def test_integration_1():
    a = make_rand_df(100, a=int, b=str, c=float, d=int, e=bool, f=str, g=str, h=float)
    eq_sqlite(
        """
        WITH
            a1 AS (
                SELECT a+1 AS a, b, c FROM a
            ),
            a2 AS (
                SELECT a,MAX(b) AS b_max, AVG(c) AS c_avg FROM a GROUP BY a
            ),
            a3 AS (
                SELECT d+2 AS d, f, g, h FROM a WHERE e
            )
        SELECT a1.a,b,c,b_max,c_avg,f,g,h FROM a1
            INNER JOIN a2 ON a1.a=a2.a
            LEFT JOIN a3 ON a1.a=a3.d
        ORDER BY a1.a NULLS FIRST, b NULLS FIRST, c NULLS FIRST, f NULLS FIRST, g NULLS FIRST, h NULLS FIRST
        """,
        a=a,
    )
