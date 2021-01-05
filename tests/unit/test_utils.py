import pytest
from dask import dataframe as dd
import pandas as pd

from dask_sql.utils import (
    is_frame,
    Pluggable,
    ParsingException,
)
from dask_sql.java import _set_or_check_java_home


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


def test_exception_parsing():
    e = ParsingException(
        "SELECT * FROM df",
        """org.apache.calcite.runtime.CalciteContextException: From line 1, column 3 to line 1, column 4: Message""",
    )

    expected = """Can not parse the given SQL: org.apache.calcite.runtime.CalciteContextException: From line 1, column 3 to line 1, column 4: Message

The problem is probably somewhere here:

\tSELECT * FROM df
\t  ^^"""
    assert str(e) == expected

    e = ParsingException(
        "SELECT * FROM df", """Lexical error at line 1, column 3.  Message""",
    )

    expected = """Can not parse the given SQL: Lexical error at line 1, column 3.  Message

The problem is probably somewhere here:

\tSELECT * FROM df
\t  ^"""
    assert str(e) == expected

    e = ParsingException(
        "SELECT *\nFROM df\nWHERE x = 3",
        """From line 1, column 3 to line 2, column 3: Message""",
    )

    expected = """Can not parse the given SQL: From line 1, column 3 to line 2, column 3: Message

The problem is probably somewhere here:

\tSELECT *
\t  ^^^^^^^
\tFROM df
\t^^^
\tWHERE x = 3"""
    assert str(e) == expected

    e = ParsingException("SELECT *", "Message",)

    assert str(e) == "Message"


def test_no_warning():
    with pytest.warns(None) as warn:
        _set_or_check_java_home()

    assert not warn
