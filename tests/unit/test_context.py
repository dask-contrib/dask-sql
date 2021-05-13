import os
import warnings

import dask.dataframe as dd
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from dask_sql import Context


def test_add_remove_tables():
    c = Context()

    data_frame = dd.from_pandas(pd.DataFrame(), npartitions=1)

    c.create_table("table", data_frame)
    assert "table" in c.tables

    c.drop_table("table")
    assert "table" not in c.tables

    with pytest.raises(KeyError):
        c.drop_table("table")

    c.create_table("table", [data_frame])
    assert "table" in c.tables


def test_deprecation_warning():
    c = Context()
    data_frame = dd.from_pandas(pd.DataFrame(), npartitions=1)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        c.register_dask_table(data_frame, "table")

        assert len(w) == 1
        assert issubclass(w[-1].category, DeprecationWarning)

    assert "table" in c.tables

    c.drop_table("table")
    assert "table" not in c.tables


def test_explain():
    c = Context()

    data_frame = dd.from_pandas(pd.DataFrame({"a": [1, 2, 3]}), npartitions=1)
    c.create_table("df", data_frame)

    sql_string = c.explain("SELECT * FROM df")

    assert (
        sql_string
        == f"LogicalProject(a=[$0]){os.linesep}  LogicalTableScan(table=[[schema, df]]){os.linesep}"
    )

    c = Context()

    data_frame = dd.from_pandas(pd.DataFrame({"a": [1, 2, 3]}), npartitions=1)

    sql_string = c.explain(
        "SELECT * FROM other_df", dataframes={"other_df": data_frame}
    )

    assert (
        sql_string
        == f"LogicalProject(a=[$0]){os.linesep}  LogicalTableScan(table=[[schema, other_df]]){os.linesep}"
    )


def test_sql():
    c = Context()

    data_frame = dd.from_pandas(pd.DataFrame({"a": [1, 2, 3]}), npartitions=1)
    c.create_table("df", data_frame)

    result = c.sql("SELECT * FROM df")
    assert isinstance(result, dd.DataFrame)
    assert_frame_equal(result.compute(), data_frame.compute())

    result = c.sql("SELECT * FROM df", return_futures=False)
    assert isinstance(result, pd.DataFrame)
    assert_frame_equal(result, data_frame.compute())

    result = c.sql("SELECT * FROM other_df", dataframes={"other_df": data_frame})
    assert isinstance(result, dd.DataFrame)
    assert_frame_equal(result.compute(), data_frame.compute())


def test_input_types(temporary_data_file):
    c = Context()
    df = pd.DataFrame({"a": [1, 2, 3]})

    def assert_correct_output():
        result = c.sql("SELECT * FROM df")
        assert isinstance(result, dd.DataFrame)
        assert_frame_equal(result.compute(), df)

    c.create_table("df", df)
    assert_correct_output()

    c.create_table("df", dd.from_pandas(df, npartitions=1))
    assert_correct_output()

    df.to_csv(temporary_data_file, index=False)
    c.create_table("df", temporary_data_file)
    assert_correct_output()

    df.to_csv(temporary_data_file, index=False)
    c.create_table("df", temporary_data_file, format="csv")
    assert_correct_output()

    df.to_parquet(temporary_data_file, index=False)
    c.create_table("df", temporary_data_file, format="parquet")
    assert_correct_output()

    with pytest.raises(AttributeError):
        c.create_table("df", temporary_data_file, format="unknown")

    strangeThing = object()

    with pytest.raises(ValueError):
        c.create_table("df", strangeThing)


def test_tables_from_stack():
    c = Context()

    assert not c._get_tables_from_stack()

    df = pd.DataFrame()

    assert "df" in c._get_tables_from_stack()

    def f():
        df2 = pd.DataFrame()

        assert "df" in c._get_tables_from_stack()
        assert "df2" in c._get_tables_from_stack()

    f()

    def g():
        df = pd.DataFrame({"a": [1]})

        assert "df" in c._get_tables_from_stack()
        assert c._get_tables_from_stack()["df"].columns == ["a"]


def test_function_adding():
    c = Context()

    assert not c.function_list
    assert not c.functions

    f = lambda x: x
    c.register_function(f, "f", [("x", int)], float)

    assert "f" in c.functions
    assert c.functions["f"] == f
    assert len(c.function_list) == 1
    assert c.function_list[0].name == "f"
    assert c.function_list[0].parameters == [("x", int)]
    assert c.function_list[0].return_type == float
    assert not c.function_list[0].aggregation

    # Without replacement
    c.register_function(f, "f", [("x", float)], int, replace=False)

    assert "f" in c.functions
    assert c.functions["f"] == f
    assert len(c.function_list) == 2
    assert c.function_list[1].name == "f"
    assert c.function_list[1].parameters == [("x", float)]
    assert c.function_list[1].return_type == int
    assert not c.function_list[1].aggregation

    # With replacement
    f = lambda x: x + 1
    c.register_function(f, "f", [("x", str)], str, replace=True)

    assert "f" in c.functions
    assert c.functions["f"] == f
    assert len(c.function_list) == 1
    assert c.function_list[0].name == "f"
    assert c.function_list[0].parameters == [("x", str)]
    assert c.function_list[0].return_type == str
    assert not c.function_list[0].aggregation


def test_aggregation_adding():
    c = Context()

    assert not c.function_list
    assert not c.functions

    f = lambda x: x
    c.register_aggregation(f, "f", [("x", int)], float)

    assert "f" in c.functions
    assert c.functions["f"] == f
    assert len(c.function_list) == 1
    assert c.function_list[0].name == "f"
    assert c.function_list[0].parameters == [("x", int)]
    assert c.function_list[0].return_type == float
    assert c.function_list[0].aggregation

    # Without replacement
    c.register_aggregation(f, "f", [("x", float)], int, replace=False)

    assert "f" in c.functions
    assert c.functions["f"] == f
    assert len(c.function_list) == 2
    assert c.function_list[1].name == "f"
    assert c.function_list[1].parameters == [("x", float)]
    assert c.function_list[1].return_type == int
    assert c.function_list[1].aggregation

    # With replacement
    f = lambda x: x + 1
    c.register_aggregation(f, "f", [("x", str)], str, replace=True)

    assert "f" in c.functions
    assert c.functions["f"] == f
    assert len(c.function_list) == 1
    assert c.function_list[0].name == "f"
    assert c.function_list[0].parameters == [("x", str)]
    assert c.function_list[0].return_type == str
    assert c.function_list[0].aggregation
