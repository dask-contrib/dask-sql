import dask.dataframe as dd
import numpy as np
import pandas as pd

from dask_sql import Context
from tests.utils import assert_eq


def test_join(c):
    return_df = c.sql(
        """
    SELECT lhs.user_id, lhs.b, rhs.c
    FROM user_table_1 AS lhs
    JOIN user_table_2 AS rhs
    ON lhs.user_id = rhs.user_id
    """
    )
    expected_df = pd.DataFrame(
        {"user_id": [1, 1, 2, 2], "b": [3, 3, 1, 3], "c": [1, 2, 3, 3]}
    )

    assert_eq(return_df, expected_df, check_index=False)


def test_join_inner(c):
    return_df = c.sql(
        """
    SELECT lhs.user_id, lhs.b, rhs.c
    FROM user_table_1 AS lhs
    INNER JOIN user_table_2 AS rhs
    ON lhs.user_id = rhs.user_id
    """
    )
    expected_df = pd.DataFrame(
        {"user_id": [1, 1, 2, 2], "b": [3, 3, 1, 3], "c": [1, 2, 3, 3]}
    )

    assert_eq(return_df, expected_df, check_index=False)


def test_join_outer(c):
    return_df = c.sql(
        """
    SELECT lhs.user_id, lhs.b, rhs.c
    FROM user_table_1 AS lhs
    FULL JOIN user_table_2 AS rhs
    ON lhs.user_id = rhs.user_id
    """
    )
    expected_df = pd.DataFrame(
        {
            # That is strange. Unfortunately, it seems dask fills in the
            # missing rows with NaN, not with NA...
            "user_id": [1, 1, 2, 2, 3, np.NaN],
            "b": [3, 3, 1, 3, 3, np.NaN],
            "c": [1, 2, 3, 3, np.NaN, 4],
        }
    )

    assert_eq(return_df, expected_df, check_index=False)


def test_join_left(c):
    return_df = c.sql(
        """
    SELECT lhs.user_id, lhs.b, rhs.c
    FROM user_table_1 AS lhs
    LEFT JOIN user_table_2 AS rhs
    ON lhs.user_id = rhs.user_id
    """
    )
    expected_df = pd.DataFrame(
        {
            # That is strange. Unfortunately, it seems dask fills in the
            # missing rows with NaN, not with NA...
            "user_id": [1, 1, 2, 2, 3],
            "b": [3, 3, 1, 3, 3],
            "c": [1, 2, 3, 3, np.NaN],
        }
    )

    assert_eq(return_df, expected_df, check_index=False)


def test_join_right(c):
    return_df = c.sql(
        """
    SELECT lhs.user_id, lhs.b, rhs.c
    FROM user_table_1 AS lhs
    RIGHT JOIN user_table_2 AS rhs
    ON lhs.user_id = rhs.user_id
    """
    )
    expected_df = pd.DataFrame(
        {
            # That is strange. Unfortunately, it seems dask fills in the
            # missing rows with NaN, not with NA...
            "user_id": [1, 1, 2, 2, np.NaN],
            "b": [3, 3, 1, 3, np.NaN],
            "c": [1, 2, 3, 3, 4],
        }
    )

    assert_eq(return_df, expected_df, check_index=False)


def test_join_cross(c, user_table_1, department_table):
    return_df = c.sql(
        """
    SELECT user_id, b, department_name
    FROM user_table_1, department_table
    """
    )

    user_table_1["key"] = 1
    department_table["key"] = 1

    expected_df = dd.merge(user_table_1, department_table, on="key").drop("key", 1)

    assert_eq(return_df, expected_df, check_index=False)


def test_join_complex(c):
    return_df = c.sql(
        """
    SELECT lhs.a, rhs.b
    FROM df_simple AS lhs
    JOIN df_simple AS rhs
    ON lhs.a < rhs.b
    """
    )
    expected_df = pd.DataFrame(
        {"a": [1, 1, 1, 2, 2, 3], "b": [1.1, 2.2, 3.3, 2.2, 3.3, 3.3]}
    )

    assert_eq(return_df, expected_df, check_index=False)

    return_df = c.sql(
        """
    SELECT lhs.a, lhs.b, rhs.a, rhs.b
    FROM df_simple AS lhs
    JOIN df_simple AS rhs
    ON lhs.a < rhs.b AND lhs.b < rhs.a
    """
    )
    expected_df = pd.DataFrame(
        {
            "lhs.a": [1, 1, 2],
            "lhs.b": [1.1, 1.1, 2.2],
            "rhs.a": [2, 3, 3],
            "rhs.b": [2.2, 3.3, 3.3],
        }
    )

    assert_eq(return_df, expected_df, check_index=False)

    return_df = c.sql(
        """
    SELECT lhs.user_id, lhs.b, rhs.user_id, rhs.c
    FROM user_table_1 AS lhs
    JOIN user_table_2 AS rhs
    ON rhs.user_id = lhs.user_id AND rhs.c - lhs.b >= 0
    """
    )
    expected_df = pd.DataFrame(
        {"lhs.user_id": [2, 2], "b": [1, 3], "rhs.user_id": [2, 2], "c": [3, 3]}
    )

    assert_eq(return_df, expected_df, check_index=False)


def test_join_literal(c):
    return_df = c.sql(
        """
    SELECT lhs.user_id, lhs.b, rhs.user_id, rhs.c
    FROM user_table_1 AS lhs
    JOIN user_table_2 AS rhs
    ON True
    """
    )
    expected_df = pd.DataFrame(
        {
            "lhs.user_id": [2, 2, 2, 2, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3],
            "b": [1, 1, 1, 1, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3],
            "rhs.user_id": [1, 1, 2, 4, 1, 1, 2, 4, 1, 1, 2, 4, 1, 1, 2, 4],
            "c": [1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4],
        }
    )

    assert_eq(return_df, expected_df, check_index=False)

    return_df = c.sql(
        """
    SELECT lhs.user_id, lhs.b, rhs.user_id, rhs.c
    FROM user_table_1 AS lhs
    JOIN user_table_2 AS rhs
    ON False
    """
    )
    expected_df = pd.DataFrame({"lhs.user_id": [], "b": [], "rhs.user_id": [], "c": []})

    assert_eq(return_df, expected_df, check_dtype=False, check_index=False)


def test_conditional_join(c):
    df1 = pd.DataFrame({"a": [1, 2, 2, 5, 6], "b": ["w", "x", "y", None, "z"]})
    df2 = pd.DataFrame({"c": [None, 3, 2, 5], "d": ["h", "i", "j", "k"]})

    expected_df = pd.merge(df1, df2, how="inner", left_on=["a"], right_on=["c"])
    expected_df = expected_df[~pd.isnull(expected_df.b)]

    c.create_table("df1", df1)
    c.create_table("df2", df2)

    actual_df = c.sql(
        """
    SELECT * FROM df1
    INNER JOIN df2 ON
    (
        a = c
        AND b IS NOT NULL
    )
    """
    )

    assert_eq(actual_df, expected_df, check_index=False, check_dtype=False)


def test_join_on_unary_cond_only(c):
    df1 = pd.DataFrame({"a": [1, 2, 2, 5, 6], "b": ["w", "x", "y", None, "z"]})
    df2 = pd.DataFrame({"c": [None, 3, 2, 5], "d": ["h", "i", "j", "k"]})

    c.create_table("df1", df1)
    c.create_table("df2", df2)

    df1 = df1.assign(common=1)
    df2 = df2.assign(common=1)

    expected_df = df1.merge(df2, on="common").drop(columns="common")
    expected_df = expected_df[~pd.isnull(expected_df.b)]

    actual_df = c.sql("SELECT * FROM df1 INNER JOIN df2 ON b IS NOT NULL")

    assert_eq(actual_df, expected_df, check_index=False, check_dtype=False)


def test_join_case_projection_subquery():
    c = Context()

    # Tables for query
    demo = pd.DataFrame({"demo_sku": [], "hd_dep_count": []})
    site_page = pd.DataFrame({"site_page_sk": [], "site_char_count": []})
    sales = pd.DataFrame(
        {"sales_hdemo_sk": [], "sales_page_sk": [], "sold_time_sk": []}
    )
    t_dim = pd.DataFrame({"t_time_sk": [], "t_hour": []})

    c.create_table("demos", demo, persist=False)
    c.create_table("site_page", site_page, persist=False)
    c.create_table("sales", sales, persist=False)
    c.create_table("t_dim", t_dim, persist=False)

    c.sql(
        """
    SELECT CASE WHEN pmc > 0.0 THEN CAST (amc AS DOUBLE) / CAST (pmc AS DOUBLE) ELSE -1.0 END AS am_pm_ratio
    FROM
    (
        SELECT SUM(amc1) AS amc, SUM(pmc1) AS pmc
        FROM
        (
            SELECT
                CASE WHEN t_hour BETWEEN 7 AND 8 THEN COUNT(1) ELSE 0 END AS amc1,
                CASE WHEN t_hour BETWEEN 19 AND 20 THEN COUNT(1) ELSE 0 END AS pmc1
            FROM sales ws
            JOIN demos hd ON (hd.demo_sku = ws.sales_hdemo_sk and hd.hd_dep_count = 5)
            JOIN site_page sp ON (sp.site_page_sk = ws.sales_page_sk and sp.site_char_count BETWEEN 5000 AND 6000)
            JOIN t_dim td ON (td.t_time_sk = ws.sold_time_sk and td.t_hour IN (7,8,19,20))
            GROUP BY t_hour
        ) cnt_am_pm
    ) sum_am_pm
    """
    ).compute()


def test_conditional_join_with_limit(c):
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    ddf = dd.from_pandas(df, 5)

    c.create_table("many_partitions", ddf)

    df = df.assign(common=1)
    expected_df = df.merge(df, on="common", suffixes=("", "0")).drop(columns="common")
    expected_df = expected_df[expected_df["a"] >= 2][:4]

    # Columns are renamed to use their fully qualified names which is more accurate
    expected_df = expected_df.rename(
        columns={"a": "df1.a", "b": "df1.b", "a0": "df2.a", "b0": "df2.b"}
    )

    actual_df = c.sql(
        """
    SELECT * FROM
        many_partitions as df1, many_partitions as df2
    WHERE
        df1."a" >= 2
    LIMIT 4
    """
    )

    assert_eq(actual_df, expected_df, check_index=False)


def test_intersect(c):

    # Join df_simple against itself
    actual_df = c.sql(
        """
    select count(*) from (
    select * from df_simple
    intersect
    select * from df_simple
    ) hot_item
    limit 100
    """
    )
    assert actual_df["COUNT(UInt8(1))"].compute()[0] == 3

    # Join df_simple against itself, and then that result against df_wide. Nothing should match so therefore result should be 0
    actual_df = c.sql(
        """
    select count(*) from (
    select a, b from df_simple
    intersect
    select a, b from df_simple
    intersect
    select a, b from df_wide
    ) hot_item
    limit 100
    """
    )
    assert len(actual_df["COUNT(UInt8(1))"]) == 0

    actual_df = c.sql(
        """
    select * from df_simple intersect select * from df_simple
    """
    )
    assert actual_df.shape[0].compute() == 3


def test_intersect_multi_col(c):
    df1 = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
    df2 = pd.DataFrame({"a": [1, 1, 1], "b": [4, 5, 6], "c": [7, 7, 7]})

    c.create_table("df1", df1)
    c.create_table("df2", df2)

    return_df = c.sql("select * from df1 intersect select * from df2")
    expected_df = pd.DataFrame(
        {
            "df1.a": [1],
            "df1.b": [4],
            "df1.c": [7],
            "df2.a": [1],
            "df2.b": [4],
            "df2.c": [7],
        }
    )
    assert_eq(return_df, expected_df, check_index=False)


def test_join_alias_w_projection(c, parquet_ddf):
    result_df = c.sql(
        "SELECT t2.c as c_y from parquet_ddf t1, parquet_ddf t2 WHERE t1.a=t2.a and t1.c='A'"
    )
    expected_df = parquet_ddf.merge(parquet_ddf, on=["a"], how="inner")
    expected_df = expected_df[expected_df["c_x"] == "A"][["c_y"]]
    assert_eq(result_df, expected_df, check_index=False)


def test_filter_columns_post_join(c):
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "c": [1, None, 2, 2, 2]})
    df2 = pd.DataFrame({"b": [1, 1, 2, 2, 3], "c": [2, 2, 2, 2, 2]})
    c.create_table("df", df)
    c.create_table("df2", df2)

    query = "SELECT SUM(df.a) as sum_a, df2.b FROM df INNER JOIN df2 ON df.c=df2.c GROUP BY df2.b"

    explain_string = c.explain(query)
    assert ("Projection: df.a, df2.b" in explain_string) or (
        "Projection: df2.b, df.a" in explain_string
    )

    result_df = c.sql(query)
    expected_df = pd.DataFrame({"sum_a": [24, 24, 12], "b": [1, 2, 3]})
    assert_eq(result_df, expected_df)
