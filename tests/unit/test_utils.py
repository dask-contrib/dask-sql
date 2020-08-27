import pytest
from dask import dataframe as dd
import pandas as pd

from dask_sql.utils import is_frame, Pluggable


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
