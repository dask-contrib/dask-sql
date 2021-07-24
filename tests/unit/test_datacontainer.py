import pandas as pd

from dask_sql.datacontainer import ColumnContainer, DataContainer, SchemaContainer


def test_cc_init():
    c = ColumnContainer(["a", "b", "c"])

    assert c.columns == ["a", "b", "c"]
    assert c.mapping() == [("a", "a"), ("b", "b"), ("c", "c")]

    c = ColumnContainer(["a", "b", "c"], {"a": "1", "b": "2", "c": "3"})

    assert c.columns == ["a", "b", "c"]
    assert c.mapping() == [("a", "1"), ("b", "2"), ("c", "3")]


def test_cc_limit_to():
    c = ColumnContainer(["a", "b", "c"])

    c2 = c.limit_to(["c", "a"])

    assert c2.columns == ["c", "a"]
    assert c2.mapping() == [("a", "a"), ("b", "b"), ("c", "c")]
    assert c.columns == ["a", "b", "c"]
    assert c.mapping() == [("a", "a"), ("b", "b"), ("c", "c")]


def test_cc_rename():
    c = ColumnContainer(["a", "b", "c"])

    c2 = c.rename({"a": "A", "b": "a"})

    assert c2.columns == ["A", "a", "c"]
    assert c2.mapping() == [("a", "b"), ("b", "b"), ("c", "c"), ("A", "a")]
    assert c.columns == ["a", "b", "c"]
    assert c.mapping() == [("a", "a"), ("b", "b"), ("c", "c")]


def test_cc_add():
    c = ColumnContainer(["a", "b", "c"])

    c2 = c.add("d")

    assert c2.columns == ["a", "b", "c", "d"]
    assert c2.mapping() == [("a", "a"), ("b", "b"), ("c", "c"), ("d", "d")]
    assert c.columns == ["a", "b", "c"]
    assert c.mapping() == [("a", "a"), ("b", "b"), ("c", "c")]

    c2 = c.add("d", "D")

    assert c2.columns == ["a", "b", "c", "d"]
    assert c2.mapping() == [("a", "a"), ("b", "b"), ("c", "c"), ("d", "D")]
    assert c.columns == ["a", "b", "c"]
    assert c.mapping() == [("a", "a"), ("b", "b"), ("c", "c")]

    c2 = c.add("d", "a")

    assert c2.columns == ["a", "b", "c", "d"]
    assert c2.mapping() == [("a", "a"), ("b", "b"), ("c", "c"), ("d", "a")]
    assert c.columns == ["a", "b", "c"]
    assert c.mapping() == [("a", "a"), ("b", "b"), ("c", "c")]

    c2 = c.add("a", "b")

    assert c2.columns == ["a", "b", "c"]
    assert c2.mapping() == [("a", "b"), ("b", "b"), ("c", "c")]
    assert c.columns == ["a", "b", "c"]
    assert c.mapping() == [("a", "a"), ("b", "b"), ("c", "c")]


def test_schema_container():
    example = SchemaContainer("example")
    example.add("experiment", pd.DataFrame())
    assert "experiment" in example.experiments

    example.add("model", ("dummy_model", "dummy_cols"))
    assert "model" in example.models

    dummy_data_container = DataContainer(pd.DataFrame(), ColumnContainer(""))
    example.add("table", dummy_data_container)
    assert "table" in example.tables

    example.add("funcs", lambda x: x + 42)
    assert "funcs" in example.functions

    example.drop("tables", "table")
    assert "table" not in example.tables
    example.drop("experiments", "experiment")
    assert "experiment" not in example.experiments
    example.drop("models", "model")
    assert "model" not in example.models
    example.drop("functions", "funcs")
    assert "funcs" not in example.functions
