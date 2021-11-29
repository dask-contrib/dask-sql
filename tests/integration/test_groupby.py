import numpy as np
import pandas as pd
import pytest
from dask import dataframe as dd
from pandas.testing import assert_frame_equal, assert_series_equal


def test_group_by(c):
    df = c.sql(
        """
    SELECT
        user_id, SUM(b) AS "S"
    FROM user_table_1
    GROUP BY user_id
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame({"user_id": [1, 2, 3], "S": [3, 4, 3]})
    assert_frame_equal(df.sort_values("user_id").reset_index(drop=True), expected_df)


def test_group_by_all(c, df):
    result_df = c.sql(
        """
    SELECT
        SUM(b) AS "S", SUM(2) AS "X"
    FROM user_table_1
    """
    )
    result_df = result_df.compute()

    expected_df = pd.DataFrame({"S": [10], "X": [8]})
    expected_df["S"] = expected_df["S"].astype("int64")
    expected_df["X"] = expected_df["X"].astype("int32")
    assert_frame_equal(result_df, expected_df)

    result_df = c.sql(
        """
        SELECT
            SUM(a) AS sum_a,
            AVG(a) AS avg_a,
            SUM(b) AS sum_b,
            AVG(b) AS avg_b,
            SUM(a)+AVG(b) AS mix_1,
            SUM(a+b) AS mix_2,
            AVG(a+b) AS mix_3
        FROM df
        """
    )
    result_df = result_df.compute()

    expected_df = pd.DataFrame(
        {
            "sum_a": [df.a.sum()],
            "avg_a": [df.a.mean()],
            "sum_b": [df.b.sum()],
            "avg_b": [df.b.mean()],
            "mix_1": [df.a.sum() + df.b.mean()],
            "mix_2": [(df.a + df.b).sum()],
            "mix_3": [(df.a + df.b).mean()],
        }
    )
    assert_frame_equal(result_df, expected_df)


def test_group_by_filtered(c):
    df = c.sql(
        """
    SELECT
        SUM(b) FILTER (WHERE user_id = 2) AS "S1",
        SUM(b) "S2"
    FROM user_table_1
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame({"S1": [4], "S2": [10]}, dtype="int64")
    assert_frame_equal(df, expected_df)


def test_group_by_filtered2(c):
    df = c.sql(
        """
    SELECT
        user_id,
        SUM(b) FILTER (WHERE user_id = 2) AS "S1",
        SUM(b) "S2"
    FROM user_table_1
    GROUP BY user_id
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame(
        {"user_id": [1, 2, 3], "S1": [np.NaN, 4.0, np.NaN], "S2": [3, 4, 3],},
    )
    assert_frame_equal(df, expected_df)

    df = c.sql(
        """
    SELECT
        SUM(b) FILTER (WHERE user_id = 2) AS "S1"
    FROM user_table_1
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame({"S1": [4]})
    assert_frame_equal(df, expected_df)


def test_group_by_case(c):
    df = c.sql(
        """
    SELECT
        user_id + 1 AS "A", SUM(CASE WHEN b = 3 THEN 1 END) AS "S"
    FROM user_table_1
    GROUP BY user_id + 1
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame({"A": [2, 3, 4], "S": [1, 1, 1]})
    # Do not check dtypes, as pandas versions are inconsistent here
    assert_frame_equal(
        df.sort_values("A").reset_index(drop=True), expected_df, check_dtype=False
    )


def test_group_by_nan(c):
    df = c.sql(
        """
    SELECT
        c
    FROM user_table_nan
    GROUP BY c
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame({"c": [3, float("nan"), 1]})
    # The dtype in pandas 1.0.5 and pandas 1.1.0 are different, so
    # we can not check here
    assert_frame_equal(
        df.sort_values("c").reset_index(drop=True),
        expected_df.sort_values("c").reset_index(drop=True),
        check_dtype=False,
    )

    df = c.sql(
        """
    SELECT
        c
    FROM user_table_inf
    GROUP BY c
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame({"c": [3, 1, float("inf")]})
    expected_df["c"] = expected_df["c"].astype("float64")
    assert_frame_equal(
        df.sort_values("c").reset_index(drop=True),
        expected_df.sort_values("c").reset_index(drop=True),
    )


def test_aggregations(c):
    df = c.sql(
        """
    SELECT
        user_id,
        EVERY(b = 3) AS e,
        BIT_AND(b) AS b,
        BIT_OR(b) AS bb,
        MIN(b) AS m,
        SINGLE_VALUE(b) AS s,
        AVG(b) AS a
    FROM user_table_1
    GROUP BY user_id
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame(
        {
            "user_id": [1, 2, 3],
            "e": [True, False, True],
            "b": [3, 1, 3],
            "bb": [3, 3, 3],
            "m": [3, 1, 3],
            "s": [3, 3, 3],
            "a": [3, 2, 3],
        }
    )
    expected_df["a"] = expected_df["a"].astype("float64")
    assert_frame_equal(df.sort_values("user_id").reset_index(drop=True), expected_df)

    df = c.sql(
        """
    SELECT
        user_id,
        EVERY(c = 3) AS e,
        BIT_AND(c) AS b,
        BIT_OR(c) AS bb,
        MIN(c) AS m,
        SINGLE_VALUE(c) AS s,
        AVG(c) AS a
    FROM user_table_2
    GROUP BY user_id
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame(
        {
            "user_id": [1, 2, 4],
            "e": [False, True, False],
            "b": [0, 3, 4],
            "bb": [3, 3, 4],
            "m": [1, 3, 4],
            "s": [1, 3, 4],
            "a": [1.5, 3, 4],
        }
    )
    assert_frame_equal(df.sort_values("user_id").reset_index(drop=True), expected_df)

    df = c.sql(
        """
    SELECT
        MAX(a) AS "max",
        MIN(a) AS "min"
    FROM string_table
    """
    )
    df = df.compute()

    expected_df = pd.DataFrame({"max": ["a normal string"], "min": ["%_%"]})
    assert_frame_equal(df.reset_index(drop=True), expected_df)


def test_stats_aggregation(c, timeseries_df):

    # # test regr_count
    regr_count = (
        c.sql(
            """
        SELECT name, count(x) filter (where y is not null) as expected,
         regr_count(y, x) as calculated from timeseries group by name
    """
        )
        .compute()
        .fillna(0)
    )

    assert_series_equal(
        regr_count["expected"],
        regr_count["calculated"],
        check_dtype=False,
        check_names=False,
    )

    # test regr_syy
    regr_syy = (
        c.sql(
            """
        SELECT name, (regr_count(y, x)*var_pop(y)) as expected, regr_syy(y, x) as calculated
        FROM timeseries WHERE x IS NOT NULL AND y IS NOT NULL GROUP BY name
    """
        )
        .compute()
        .fillna(0)
    )
    assert_series_equal(
        regr_syy["expected"],
        regr_syy["calculated"],
        check_dtype=False,
        check_names=False,
    )

    # test regr_sxx
    regr_sxx = (
        c.sql(
            """
        SELECT name,(regr_count(y, x)*var_pop(x)) as expected, regr_sxx(y,x) as calculated
        FROM timeseries WHERE x IS NOT NULL AND y IS NOT NULL GROUP BY name
    """
        )
        .compute()
        .fillna(0)
    )
    assert_series_equal(
        regr_sxx["expected"],
        regr_sxx["calculated"],
        check_dtype=False,
        check_names=False,
    )

    # test covar_pop
    covar_pop = (
        c.sql(
            """
        with temp_agg as (
        select name,avg(y) filter (where x is not null) as avg_y,
        avg(x) filter (where y is not null) as avg_x
        from timeseries group by name
        )
        select ts.name,sum((y - avg_y) * (x - avg_x)) /regr_count(y, x) as expected,
                covar_pop(y,x) as calculated from timeseries as ts
                join temp_agg as ta on ts.name =ta.name
                group by ts.name
    """
        )
        .compute()
        .fillna(0)
    )
    assert_series_equal(
        covar_pop["expected"],
        covar_pop["calculated"],
        check_dtype=False,
        check_names=False,
    )

    # test covar_samp
    covar_samp = (
        c.sql(
            """
        with temp_agg as (
        select name,avg(y) filter (where x is not null) as avg_y,
        avg(x) filter (where y is not null) as avg_x
        from timeseries group by name
        )

        select ts.name,sum((y - avg_y) * (x - avg_x)) /(regr_count(y, x)-1) as expected,
                covar_samp(y,x) as calculated from timeseries as ts
                join temp_agg as ta on ts.name =ta.name
                group by ts.name
    """
        )
        .compute()
        .fillna(0)
    )
    assert_series_equal(
        covar_samp["expected"],
        covar_samp["calculated"],
        check_dtype=False,
        check_names=False,
    )


@pytest.mark.parametrize(
    "input_table",
    ["user_table_1", pytest.param("gpu_user_table_1", marks=pytest.mark.gpu),],
)
@pytest.mark.parametrize("split_out", [None, 2, 4])
def test_groupby_split_out(c, input_table, split_out, request):
    user_table = request.getfixturevalue(input_table)
    c.set_config(("dask.groupby.aggregate.split_out", split_out))
    df = c.sql(
        f"""
        SELECT
        user_id, SUM(b) AS "S"
        FROM {input_table}
        GROUP BY user_id
        """
    )
    expected_df = (
        user_table.groupby(by="user_id").agg({"b": "sum"}).reset_index(drop=False)
    )
    expected_df = expected_df.rename(columns={"b": "S"})
    expected_df = expected_df.sort_values("user_id")
    assert df.npartitions == split_out if split_out else 1
    dd.assert_eq(df.compute().sort_values("user_id"), expected_df, check_index=False)
    c.drop_config("dask.groupby.aggregate.split_out")


@pytest.mark.parametrize(
    "gpu,split_every,expected_keys",
    [
        (False, 2, 74),
        (False, 3, 68),
        (False, 4, 64),
        pytest.param(True, 2, 91, marks=pytest.mark.gpu),
        pytest.param(True, 3, 85, marks=pytest.mark.gpu),
        pytest.param(True, 4, 81, marks=pytest.mark.gpu),
    ],
)
def test_groupby_split_every(c, gpu, split_every, expected_keys):
    xd = pytest.importorskip("cudf") if gpu else pd
    input_ddf = dd.from_pandas(
        xd.DataFrame({"user_id": [1, 2, 3, 4] * 16, "b": [5, 6, 7, 8] * 16}),
        npartitions=16,
    )  # Need an input with multiple partitions to demonstrate split_every

    c.create_table("split_every_input", input_ddf)
    c.set_config(("dask.groupby.aggregate.split_every", split_every))

    df = c.sql(
        """
        SELECT
        user_id, SUM(b) AS "S"
        FROM split_every_input
        GROUP BY user_id
        """
    )
    expected_df = (
        input_ddf.groupby(by="user_id")
        .agg({"b": "sum"}, split_every=split_every)
        .reset_index(drop=False)
        .rename(columns={"b": "S"})
        .sort_values("user_id")
    )

    assert len(df.dask.keys()) == expected_keys
    dd.assert_eq(df, expected_df, check_index=False)

    c.drop_config("dask.groupby.aggregate.split_every")
    c.drop_table("split_every_input")
