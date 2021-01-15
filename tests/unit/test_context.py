import warnings
import os

import pytest
import dask.dataframe as dd
import pandas as pd
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


def test_sql():
    c = Context()

    data_frame = dd.from_pandas(pd.DataFrame({"a": [1, 2, 3]}), npartitions=1)
    c.create_table("df", data_frame)

    result = c.sql("SELECT * FROM df")
    assert isinstance(result, dd.DataFrame)

    result = c.sql("SELECT * FROM df", return_futures=False)
    assert isinstance(result, pd.DataFrame)


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
