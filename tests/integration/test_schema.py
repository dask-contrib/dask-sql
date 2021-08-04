import dask.dataframe as dd
import numpy as np
import pytest
from pandas.testing import assert_frame_equal

from dask_sql.utils import ParsingException


def test_table_schema(c, df):
    original_df = c.sql("SELECT * FROM df").compute()

    assert_frame_equal(original_df, c.sql("SELECT * FROM root.df").compute())

    c.sql("CREATE SCHEMA foo")
    assert_frame_equal(original_df, c.sql("SELECT * FROM df").compute())

    c.sql('USE SCHEMA "foo"')
    assert_frame_equal(original_df, c.sql("SELECT * FROM root.df").compute())

    c.sql("CREATE TABLE bar AS TABLE root.df")
    assert_frame_equal(original_df, c.sql("SELECT * FROM bar").compute())

    with pytest.raises(KeyError):
        c.sql("CREATE TABLE other.bar AS TABLE df")

    c.sql('USE SCHEMA "root"')
    assert_frame_equal(original_df, c.sql("SELECT * FROM foo.bar").compute())

    with pytest.raises(ParsingException):
        c.sql("SELECT * FROM bar")

    c.sql("DROP SCHEMA foo")

    with pytest.raises(ParsingException):
        c.sql("SELECT * FROM foo.bar")


def test_function(c):
    c.sql("CREATE SCHEMA other")
    c.sql("USE SCHEMA root")

    def f(x):
        return x ** 2

    c.register_function(f, "f", [("x", np.float64)], np.float64, schema_name="other")

    with pytest.raises(ParsingException):
        c.sql("SELECT F(a) AS a FROM df")

    c.sql("SELECT other.F(a) AS a FROM df")

    c.sql("USE SCHEMA other")
    c.sql("SELECT F(a) AS a FROM root.df")

    c.sql("USE SCHEMA root")
    fagg = dd.Aggregation("f", lambda x: x.sum(), lambda x: x.sum())
    c.register_aggregation(
        fagg, "fagg", [("x", np.float64)], np.float64, schema_name="other"
    )

    with pytest.raises(ParsingException):
        c.sql("SELECT FAGG(b) AS test FROM df")

    c.sql("SELECT other.FAGG(b) AS test FROM df")

    c.sql("USE SCHEMA other")
    c.sql("SELECT FAGG(b) AS test FROM root.df")


def test_create_schema(c):
    c.sql("CREATE SCHEMA new_schema")
    assert "new_schema" in c.schema

    with pytest.raises(RuntimeError):
        c.sql("CREATE SCHEMA new_schema")

    c.sql("CREATE OR REPLACE SCHEMA new_schema")
    c.sql("CREATE SCHEMA IF NOT EXISTS new_schema")


def test_drop_schema(c):
    with pytest.raises(RuntimeError):
        c.sql("DROP SCHEMA new_schema")

    c.sql("DROP SCHEMA IF EXISTS new_schema")

    c.sql("CREATE SCHEMA new_schema")
    c.sql("DROP SCHEMA IF EXISTS new_schema")

    with pytest.raises(RuntimeError):
        c.sql("USE SCHEMA new_schema")

    with pytest.raises(RuntimeError):
        c.sql("DROP SCHEMA root")

    c.sql("CREATE SCHEMA example")
    c.sql("USE SCHEMA example")
    c.sql("DROP SCHEMA example")
    assert c.schema_name == c.DEFAULT_SCHEMA_NAME
    assert "example" not in c.schema
