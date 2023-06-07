import pandas as pd
import pytest
from dask import dataframe as dd
from dask.utils_test import hlg_layer

from dask_sql._compat import PQ_IS_SUPPORT, PQ_NOT_IN_SUPPORT
from dask_sql.physical.utils.filter import attempt_predicate_pushdown
from dask_sql.utils import Pluggable, is_frame


def test_is_frame_for_frame():
    df = dd.from_pandas(pd.DataFrame({"a": [1]}), npartitions=1)
    assert is_frame(df)


def test_is_frame_for_none():
    assert not is_frame(None)


def test_is_frame_for_number():
    assert not is_frame(3)
    assert not is_frame(3.5)


class PluginTest1(Pluggable):
    pass


class PluginTest2(Pluggable):
    pass


def test_add_plugin():
    PluginTest1.add_plugin("some_key", "value")

    assert PluginTest1.get_plugin("some_key") == "value"
    assert PluginTest1().get_plugin("some_key") == "value"

    with pytest.raises(KeyError):
        PluginTest2.get_plugin("some_key")


def test_overwrite():
    PluginTest1.add_plugin("some_key", "value")

    assert PluginTest1.get_plugin("some_key") == "value"
    assert PluginTest1().get_plugin("some_key") == "value"

    PluginTest1.add_plugin("some_key", "value_2")

    assert PluginTest1.get_plugin("some_key") == "value_2"
    assert PluginTest1().get_plugin("some_key") == "value_2"

    PluginTest1.add_plugin("some_key", "value_3", replace=False)

    assert PluginTest1.get_plugin("some_key") == "value_2"
    assert PluginTest1().get_plugin("some_key") == "value_2"


def test_predicate_pushdown_simple(parquet_ddf):
    filtered_df = parquet_ddf[parquet_ddf["a"] > 1]
    pushdown_df = attempt_predicate_pushdown(filtered_df)
    got_filters = hlg_layer(pushdown_df.dask, "read-parquet").creation_info["kwargs"][
        "filters"
    ]
    got_filters = frozenset(frozenset(v) for v in got_filters)
    expected_filters = [[("a", ">", 1)]]
    expected_filters = frozenset(frozenset(v) for v in expected_filters)
    assert got_filters == expected_filters


def test_predicate_pushdown_logical(parquet_ddf):
    filtered_df = parquet_ddf[
        (parquet_ddf["a"] > 1) & (parquet_ddf["b"] < 2) | (parquet_ddf["a"] == -1)
    ]

    pushdown_df = attempt_predicate_pushdown(filtered_df)
    got_filters = hlg_layer(pushdown_df.dask, "read-parquet").creation_info["kwargs"][
        "filters"
    ]
    got_filters = frozenset(frozenset(v) for v in got_filters)
    expected_filters = [[("a", ">", 1), ("b", "<", 2)], [("a", "==", -1)]]
    expected_filters = frozenset(frozenset(v) for v in expected_filters)
    assert got_filters == expected_filters


@pytest.mark.skipif(
    not PQ_NOT_IN_SUPPORT,
    reason="Requires https://github.com/dask/dask/pull/10320",
)
def test_predicate_pushdown_in(parquet_ddf):
    filtered_df = parquet_ddf[
        (parquet_ddf["a"] > 1) & (parquet_ddf["b"] < 2)
        | (parquet_ddf["a"] == -1) & parquet_ddf["c"].isin(("A", "B", "C"))
        | ~parquet_ddf["b"].isin((5, 6, 7))
    ]
    pushdown_df = attempt_predicate_pushdown(filtered_df)
    got_filters = hlg_layer(pushdown_df.dask, "read-parquet").creation_info["kwargs"][
        "filters"
    ]
    got_filters = frozenset(frozenset(v) for v in got_filters)
    expected_filters = [
        [("b", "<", 2), ("a", ">", 1)],
        [("a", "==", -1), ("c", "in", ("A", "B", "C"))],
        [("b", "not in", (5, 6, 7))],
    ]
    expected_filters = frozenset(frozenset(v) for v in expected_filters)
    assert got_filters == expected_filters


@pytest.mark.skipif(
    not PQ_IS_SUPPORT,
    reason="Requires dask>=2023.3.1",
)
def test_predicate_pushdown_isna(parquet_ddf):
    filtered_df = parquet_ddf[
        (parquet_ddf["a"] > 1) & (parquet_ddf["b"] < 2)
        | (parquet_ddf["a"] == -1) & ~parquet_ddf["c"].isna()
        | parquet_ddf["b"].isna()
    ]
    pushdown_df = attempt_predicate_pushdown(filtered_df)
    got_filters = hlg_layer(pushdown_df.dask, "read-parquet").creation_info["kwargs"][
        "filters"
    ]
    got_filters = frozenset(frozenset(v) for v in got_filters)
    expected_filters = [
        [("b", "<", 2), ("a", ">", 1)],
        [("a", "==", -1), ("c", "is not", None)],
        [("b", "is", None)],
    ]
    expected_filters = frozenset(frozenset(v) for v in expected_filters)
    assert got_filters == expected_filters
