import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal


def test_join(c):
    df = c.sql(
        "SELECT lhs.user_id, lhs.b, rhs.c FROM user_table_1 AS lhs JOIN user_table_2 AS rhs ON lhs.user_id = rhs.user_id"
    )
    df = df.compute()

    expected_df = pd.DataFrame(
        {"user_id": [1, 1, 2, 2], "b": [3, 3, 1, 3], "c": [1, 2, 3, 3]}
    )
    assert_frame_equal(
        df.sort_values(["user_id", "b", "c"]).reset_index(drop=True), expected_df,
    )


def test_join_inner(c):
    df = c.sql(
        "SELECT lhs.user_id, lhs.b, rhs.c FROM user_table_1 AS lhs INNER JOIN user_table_2 AS rhs ON lhs.user_id = rhs.user_id"
    )
    df = df.compute()

    expected_df = pd.DataFrame(
        {"user_id": [1, 1, 2, 2], "b": [3, 3, 1, 3], "c": [1, 2, 3, 3]}
    )
    assert_frame_equal(
        df.sort_values(["user_id", "b", "c"]).reset_index(drop=True), expected_df,
    )


def test_join_outer(c):
    df = c.sql(
        "SELECT lhs.user_id, lhs.b, rhs.c FROM user_table_1 AS lhs FULL JOIN user_table_2 AS rhs ON lhs.user_id = rhs.user_id"
    )
    df = df.compute()

    expected_df = pd.DataFrame(
        {
            # That is strange. Unfortunately, it seems dask fills in the
            # missing rows with NaN, not with NA...
            "user_id": [1, 1, 2, 2, 3, np.NaN],
            "b": [3, 3, 1, 3, 3, np.NaN],
            "c": [1, 2, 3, 3, np.NaN, 4],
        }
    )
    assert_frame_equal(
        df.sort_values(["user_id", "b", "c"]).reset_index(drop=True), expected_df
    )


def test_join_left(c):
    df = c.sql(
        "SELECT lhs.user_id, lhs.b, rhs.c FROM user_table_1 AS lhs LEFT JOIN user_table_2 AS rhs ON lhs.user_id = rhs.user_id"
    )
    df = df.compute()

    expected_df = pd.DataFrame(
        {
            # That is strange. Unfortunately, it seems dask fills in the
            # missing rows with NaN, not with NA...
            "user_id": [1, 1, 2, 2, 3],
            "b": [3, 3, 1, 3, 3],
            "c": [1, 2, 3, 3, np.NaN],
        }
    )
    assert_frame_equal(
        df.sort_values(["user_id", "b", "c"]).reset_index(drop=True), expected_df,
    )


def test_join_right(c):
    df = c.sql(
        "SELECT lhs.user_id, lhs.b, rhs.c FROM user_table_1 AS lhs RIGHT JOIN user_table_2 AS rhs ON lhs.user_id = rhs.user_id"
    )
    df = df.compute()

    expected_df = pd.DataFrame(
        {
            # That is strange. Unfortunately, it seems dask fills in the
            # missing rows with NaN, not with NA...
            "user_id": [1, 1, 2, 2, np.NaN],
            "b": [3, 3, 1, 3, np.NaN],
            "c": [1, 2, 3, 3, 4],
        }
    )
    assert_frame_equal(
        df.sort_values(["user_id", "b", "c"]).reset_index(drop=True), expected_df,
    )


def test_join_complex(c):
    df = c.sql(
        "SELECT lhs.a, rhs.b FROM df_simple AS lhs JOIN df_simple AS rhs ON lhs.a < rhs.b",
    )
    df = df.compute()

    df_expected = pd.DataFrame(
        {"a": [1, 1, 1, 2, 2, 3], "b": [1.1, 2.2, 3.3, 2.2, 3.3, 3.3]}
    )

    assert_frame_equal(df.sort_values(["a", "b"]).reset_index(drop=True), df_expected)

    df = c.sql(
        """
            SELECT lhs.a, lhs.b, rhs.a, rhs.b
            FROM
                df_simple AS lhs
            JOIN df_simple AS rhs
            ON lhs.a < rhs.b AND lhs.b < rhs.a
        """
    )
    df = df.compute()

    df_expected = pd.DataFrame(
        {"a": [1, 1, 2], "b": [1.1, 1.1, 2.2], "a0": [2, 3, 3], "b0": [2.2, 3.3, 3.3],}
    )

    assert_frame_equal(df.sort_values(["a", "b0"]).reset_index(drop=True), df_expected)


def test_join_complex_2(c):
    df = c.sql(
        """
    SELECT
        lhs.user_id, lhs.b, rhs.user_id, rhs.c
    FROM user_table_1 AS lhs
    JOIN user_table_2 AS rhs
        ON rhs.user_id = lhs.user_id AND rhs.c - lhs.b >= 0
    """
    )

    df = df.compute()

    df_expected = pd.DataFrame(
        {"user_id": [2, 2], "b": [1, 3], "user_id0": [2, 2], "c": [3, 3]}
    )

    assert_frame_equal(df.sort_values("b").reset_index(drop=True), df_expected)


def test_join_literal(c):
    df = c.sql(
        """
    SELECT
        lhs.user_id, lhs.b, rhs.user_id, rhs.c
    FROM user_table_1 AS lhs
    JOIN user_table_2 AS rhs
        ON True
    """
    )

    df = df.compute()

    df_expected = pd.DataFrame(
        {
            "user_id": [2, 2, 2, 2, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3],
            "b": [1, 1, 1, 1, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3],
            "user_id0": [1, 1, 2, 4, 1, 1, 2, 4, 1, 1, 2, 4, 1, 1, 2, 4],
            "c": [1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4],
        }
    )

    assert_frame_equal(
        df.sort_values(["b", "user_id", "user_id0"]).reset_index(drop=True),
        df_expected,
    )

    df = c.sql(
        """
    SELECT
        lhs.user_id, lhs.b, rhs.user_id, rhs.c
    FROM user_table_1 AS lhs
    JOIN user_table_2 AS rhs
        ON False
    """
    )

    df = df.compute()

    df_expected = pd.DataFrame({"user_id": [], "b": [], "user_id0": [], "c": []})

    assert_frame_equal(df.reset_index(), df_expected.reset_index(), check_dtype=False)


def test_join_lricomplex(c):
    # ---------- Panel data (equality and inequality conditions)

    # Correct answer
    dfcorrpn = pd.DataFrame(
        [
            [0, 1, pd.NA, 331, "c1", 3.1, pd.Timestamp("2003-01-01"), 0, 2, pd.NA, 110, "a1", 1.1, pd.Timestamp("2001-01-01")],
            [0, 2, pd.NA, 332, "c2", 3.2, pd.Timestamp("2003-02-01"), 0, 2, pd.NA, 110, "a1", 1.1, pd.Timestamp("2001-01-01")],
            [0, 3, pd.NA, 333, "c3", 3.3, pd.Timestamp("2003-03-01"), pd.NA, pd.NA, pd.NA, pd.NA, np.nan, np.nan, pd.NaT],
            [1, 3, pd.NA, 334, "c4", np.nan, pd.Timestamp("2003-04-01"), 2, 5, pd.NA, 112, "a3", np.nan, pd.Timestamp("2001-03-01")],
            [1, 4, 35, 335, "c5", np.nan, pd.Timestamp("2003-05-01"), 2, 5, pd.NA, 112, "a3", np.nan, pd.Timestamp("2001-03-01")],
            [1, 4, 35, 335, "c5", np.nan, pd.Timestamp("2003-05-01"), 4, 6, 13, 113, "a4", np.nan, pd.Timestamp("2001-04-01")],
            [2, 1, 36, 336, "c6", np.nan, pd.Timestamp("2003-06-01"), pd.NA, pd.NA, pd.NA, pd.NA, np.nan, np.nan, pd.NaT],
            [2, 3, 37, 337, "c7", np.nan, pd.NaT, pd.NA, pd.NA, pd.NA, pd.NA, np.nan, np.nan, pd.NaT],
            [3, 2, 38, 338, "c8", 3.8, pd.NaT, 1, 2, 14, 114, "a5", np.nan, pd.NaT],
            [3, 2, 39, 339, "c9", 3.9, pd.NaT, 1, 2, 14, 114, "a5", np.nan, pd.NaT],
            [3, 2, 38, 338, "c8", 3.8, pd.NaT, 2, 3, 15, 115, "a6", 1.6, pd.NaT],
            [3, 2, 39, 339, "c9", 3.9, pd.NaT, 2, 3, 15, 115, "a6", 1.6, pd.NaT]
        ],
        columns=[
            "ids",
            "dates",
            "pn_nullint",
            "pn_int",
            "pn_str",
            "pn_float",
            "pn_date",
            "startdate",
            "enddate",
            "lk_nullint",
            "lk_int",
            "lk_str",
            "lk_float",
            "lk_date"
        ],
    )
    change_types = {
        "pn_nullint": "Int32",
        "lk_nullint": "Int32",
        "startdate": "Int64",
        "enddate": "Int64",
        "lk_int": "Int64",
        "pn_str": "string",
        "lk_str": "string"
    }
    for k, v in change_types.items():
        dfcorrpn[k] = dfcorrpn[k].astype(v)

    # Left Join
    querypnl = """
        select a.*, b.startdate, b.enddate, b.lk_nullint, b.lk_int, b.lk_str,
            b.lk_float, b.lk_date
        from user_table_pn a left join user_table_lk b
        on a.ids=b.id and b.startdate<=a.dates and a.dates<=b.enddate
        """
    dftestpnl = (
        c.sql(querypnl).compute().sort_values(["ids", "dates", "startdate", "enddate"])
    )
    assert_frame_equal(
        dftestpnl.reset_index(drop=True), dfcorrpn.reset_index(drop=True)
    )

    # Right Join
    querypnr = """
        select b.*, a.startdate, a.enddate, a.lk_nullint, a.lk_int, a.lk_str,
            a.lk_float, a.lk_date
        from user_table_lk a right join user_table_pn b
        on b.ids=a.id and a.startdate<=b.dates and b.dates<=a.enddate
        """
    dftestpnr = (
        c.sql(querypnr).compute().sort_values(["ids", "dates", "startdate", "enddate"])
    )
    assert_frame_equal(
        dftestpnr.reset_index(drop=True), dfcorrpn.reset_index(drop=True)
    )

    # Inner Join
    querypni = """
        select a.*, b.startdate, b.enddate, b.lk_nullint, b.lk_int, b.lk_str,
            b.lk_float, b.lk_date
        from user_table_pn a inner join user_table_lk b
        on a.ids=b.id and b.startdate<=a.dates and a.dates<=b.enddate
        """
    dftestpni = (
        c.sql(querypni).compute().sort_values(["ids", "dates", "startdate", "enddate"])
    )
    assert_frame_equal(
        dftestpni.reset_index(drop=True),
        dfcorrpn.dropna(subset=["startdate"])
        .assign(
            startdate=lambda x: x["startdate"].astype("int64"),
            enddate=lambda x: x["enddate"].astype("int64"),
            lk_int=lambda x: x["lk_int"].astype("int64")
        )
        .reset_index(drop=True)
    )


    # ---------- Time-series data (inequality condition only)

    # Correct answer
    dfcorrts = pd.DataFrame(
        [
            [3, pd.NA, 221, "b1", 2.1, pd.Timestamp("2002-01-01"), 2, 5, pd.NA, 112, "a3", np.nan, pd.Timestamp("2001-03-01")],
            [4, 22, 222, "b2", np.nan, pd.Timestamp("2002-02-01"), 2, 5, pd.NA, 112, "a3", np.nan, pd.Timestamp("2001-03-01")],
            [4, 22, 222, "b2", np.nan, pd.Timestamp("2002-02-01"), 4, 6, 13, 113, "a4", np.nan, pd.Timestamp("2001-04-01")],
            [7, 23, 223, "b3", 2.3, pd.NaT, pd.NA, pd.NA, pd.NA, pd.NA, np.nan, np.nan, pd.NaT],
        ],
        columns=[
            "dates", 
            "ts_nullint",
            "ts_int", 
            "ts_str",
            "ts_float",
            "ts_date",
            "startdate",
            "enddate",
            "lk_nullint",
            "lk_int",
            "lk_str", 
            "lk_float",
            "lk_date"
        ],
    )
    change_types = {
        "ts_nullint": "Int32",
        "lk_nullint": "Int32",
        "startdate": "Int64",
        "enddate": "Int64",
        "lk_int": "Int64",
        "lk_str": "string",
        "ts_str": "string"
    }
    for k, v in change_types.items():
        dfcorrts[k] = dfcorrts[k].astype(v)

    # Left Join
    querytsl = """
        select a.*, b.startdate, b.enddate, b.lk_nullint, b.lk_int, b.lk_str,
            b.lk_float, b.lk_date
        from user_table_ts a left join user_table_lk2 b
        on b.startdate<=a.dates and a.dates<=b.enddate
    """
    dftesttsl = c.sql(querytsl).compute().sort_values(["dates", "startdate", "enddate"])
    assert_frame_equal(
        dftesttsl.reset_index(drop=True), dfcorrts.reset_index(drop=True)
    )

    # Right Join
    querytsr = """
        select b.*, a.startdate, a.enddate, a.lk_nullint, a.lk_int, a.lk_str,
            a.lk_float, a.lk_date
        from user_table_lk2 a right join user_table_ts b
        on a.startdate<=b.dates and b.dates<=a.enddate
    """
    dftesttsr = c.sql(querytsr).compute().sort_values(["dates", "startdate", "enddate"])
    assert_frame_equal(
        dftesttsr.reset_index(drop=True), dfcorrts.reset_index(drop=True)
    )

    # Inner Join
    querytsi = """
        select a.*, b.startdate, b.enddate, b.lk_nullint, b.lk_int, b.lk_str,
            b.lk_float, b.lk_date
        from user_table_ts a inner join user_table_lk2 b
        on b.startdate<=a.dates and a.dates<=b.enddate
    """
    dftesttsi = c.sql(querytsi).compute().sort_values(["dates", "startdate", "enddate"])
    assert_frame_equal(
        dftesttsi.reset_index(drop=True),
        dfcorrts.dropna(subset=["startdate"])
        .assign(
            startdate=lambda x: x["startdate"].astype("int64"),
            enddate=lambda x: x["enddate"].astype("int64"),
            lk_int=lambda x: x["lk_int"].astype("int64")
        )
        .reset_index(drop=True)
    )
    