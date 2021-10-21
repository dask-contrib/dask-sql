import os
import warnings

import dask.dataframe as dd
import pandas as pd
import pytest

from dask_sql import Context

try:
    import cudf
    import dask_cudf
except ImportError:
    cudf = None
    dask_cudf = None


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_add_remove_tables(gpu):
    c = Context()

    data_frame = dd.from_pandas(pd.DataFrame(), npartitions=1)

    c.create_table("table", data_frame, gpu=gpu)
    assert "table" in c.schema[c.schema_name].tables

    c.drop_table("table")
    assert "table" not in c.schema[c.schema_name].tables

    with pytest.raises(KeyError):
        c.drop_table("table")

    c.create_table("table", [data_frame], gpu=gpu)
    assert "table" in c.schema[c.schema_name].tables


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_deprecation_warning(gpu):
    c = Context()
    data_frame = dd.from_pandas(pd.DataFrame(), npartitions=1)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        c.register_dask_table(data_frame, "table", gpu=gpu)

        assert len(w) == 1
        assert issubclass(w[-1].category, DeprecationWarning)

    assert "table" in c.schema[c.schema_name].tables

    c.drop_table("table")
    assert "table" not in c.schema[c.schema_name].tables


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_explain(gpu):
    c = Context()

    data_frame = dd.from_pandas(pd.DataFrame({"a": [1, 2, 3]}), npartitions=1)
    c.create_table("df", data_frame, gpu=gpu)

    sql_string = c.explain("SELECT * FROM df")

    assert (
        sql_string
        == f"LogicalProject(a=[$0]){os.linesep}  LogicalTableScan(table=[[root, df]]){os.linesep}"
    )

    c = Context()

    data_frame = dd.from_pandas(pd.DataFrame({"a": [1, 2, 3]}), npartitions=1)

    if gpu:
        data_frame = dask_cudf.from_dask_dataframe(data_frame)

    sql_string = c.explain(
        "SELECT * FROM other_df", dataframes={"other_df": data_frame}
    )

    assert (
        sql_string
        == f"LogicalProject(a=[$0]){os.linesep}  LogicalTableScan(table=[[root, other_df]]){os.linesep}"
    )


@pytest.mark.parametrize(
    "gpu",
    [
        False,
        pytest.param(
            True,
            marks=(
                pytest.mark.gpu,
                pytest.mark.xfail(reason="create_table(gpu=True) doesn't work"),
            ),
        ),
    ],
)
def test_sql(gpu):
    c = Context()

    data_frame = dd.from_pandas(pd.DataFrame({"a": [1, 2, 3]}), npartitions=1)
    c.create_table("df", data_frame, gpu=gpu)

    result = c.sql("SELECT * FROM df")
    assert isinstance(result, dd.DataFrame if not gpu else dask_cudf.DataFrame)
    dd.assert_eq(result, data_frame)

    result = c.sql("SELECT * FROM df", return_futures=False)
    assert isinstance(result, pd.DataFrame if not gpu else cudf.DataFrame)
    dd.assert_eq(result, data_frame)

    if gpu:
        data_frame = dask_cudf.from_dask_dataframe(data_frame)
    result = c.sql("SELECT * FROM other_df", dataframes={"other_df": data_frame})
    assert isinstance(result, dd.DataFrame if not gpu else dask_cudf.DataFrame)
    dd.assert_eq(result, data_frame)


@pytest.mark.parametrize(
    "gpu",
    [
        False,
        pytest.param(
            True,
            marks=(
                pytest.mark.gpu,
                pytest.mark.xfail(reason="create_table(gpu=True) doesn't work"),
            ),
        ),
    ],
)
def test_input_types(temporary_data_file, gpu):
    c = Context()
    df = pd.DataFrame({"a": [1, 2, 3]})

    def assert_correct_output(gpu):
        result = c.sql("SELECT * FROM df")
        assert isinstance(result, dd.DataFrame if not gpu else dask_cudf.DataFrame)
        dd.assert_eq(result, df)

    c.create_table("df", df, gpu=gpu)
    assert_correct_output(gpu=gpu)

    c.create_table("df", dd.from_pandas(df, npartitions=1), gpu=gpu)
    assert_correct_output(gpu=gpu)

    df.to_csv(temporary_data_file, index=False)
    c.create_table("df", temporary_data_file, gpu=gpu)
    assert_correct_output(gpu=gpu)

    df.to_csv(temporary_data_file, index=False)
    c.create_table("df", temporary_data_file, format="csv", gpu=gpu)
    assert_correct_output(gpu=gpu)

    df.to_parquet(temporary_data_file, index=False)
    c.create_table("df", temporary_data_file, format="parquet", gpu=gpu)
    assert_correct_output(gpu=gpu)

    with pytest.raises(AttributeError):
        c.create_table("df", temporary_data_file, format="unknown", gpu=gpu)

    strangeThing = object()

    with pytest.raises(ValueError):
        c.create_table("df", strangeThing, gpu=gpu)


@pytest.mark.parametrize(
    "gpu",
    [
        False,
        pytest.param(
            True,
            marks=(
                pytest.mark.gpu,
                pytest.mark.xfail(
                    reason="GPU tables aren't picked up by _get_tables_from_stack"
                ),
            ),
        ),
    ],
)
def test_tables_from_stack(gpu):
    c = Context()

    assert not c._get_tables_from_stack()

    df = pd.DataFrame() if not gpu else cudf.DataFrame()

    assert "df" in c._get_tables_from_stack()

    def f(gpu):
        df2 = pd.DataFrame() if not gpu else cudf.DataFrame()

        assert "df" in c._get_tables_from_stack()
        assert "df2" in c._get_tables_from_stack()

    f(gpu=gpu)

    def g(gpu=gpu):
        df = pd.DataFrame({"a": [1]}) if not gpu else cudf.DataFrame({"a": [1]})

        assert "df" in c._get_tables_from_stack()
        assert c._get_tables_from_stack()["df"].columns == ["a"]

    g(gpu=gpu)


def test_function_adding():
    c = Context()

    assert not c.schema[c.schema_name].function_lists
    assert not c.schema[c.schema_name].functions

    f = lambda x: x
    c.register_function(f, "f", [("x", int)], float)

    assert "f" in c.schema[c.schema_name].functions
    assert c.schema[c.schema_name].functions["f"].func == f
    assert len(c.schema[c.schema_name].function_lists) == 2
    assert c.schema[c.schema_name].function_lists[0].name == "F"
    assert c.schema[c.schema_name].function_lists[0].parameters == [("x", int)]
    assert c.schema[c.schema_name].function_lists[0].return_type == float
    assert not c.schema[c.schema_name].function_lists[0].aggregation
    assert c.schema[c.schema_name].function_lists[1].name == "f"
    assert c.schema[c.schema_name].function_lists[1].parameters == [("x", int)]
    assert c.schema[c.schema_name].function_lists[1].return_type == float
    assert not c.schema[c.schema_name].function_lists[1].aggregation

    # Without replacement
    c.register_function(f, "f", [("x", float)], int, replace=False)

    assert "f" in c.schema[c.schema_name].functions
    assert c.schema[c.schema_name].functions["f"].func == f
    assert len(c.schema[c.schema_name].function_lists) == 4
    assert c.schema[c.schema_name].function_lists[2].name == "F"
    assert c.schema[c.schema_name].function_lists[2].parameters == [("x", float)]
    assert c.schema[c.schema_name].function_lists[2].return_type == int
    assert not c.schema[c.schema_name].function_lists[2].aggregation
    assert c.schema[c.schema_name].function_lists[3].name == "f"
    assert c.schema[c.schema_name].function_lists[3].parameters == [("x", float)]
    assert c.schema[c.schema_name].function_lists[3].return_type == int
    assert not c.schema[c.schema_name].function_lists[3].aggregation

    # With replacement
    f = lambda x: x + 1
    c.register_function(f, "f", [("x", str)], str, replace=True)

    assert "f" in c.schema[c.schema_name].functions
    assert c.schema[c.schema_name].functions["f"].func == f
    assert len(c.schema[c.schema_name].function_lists) == 2
    assert c.schema[c.schema_name].function_lists[0].name == "F"
    assert c.schema[c.schema_name].function_lists[0].parameters == [("x", str)]
    assert c.schema[c.schema_name].function_lists[0].return_type == str
    assert not c.schema[c.schema_name].function_lists[0].aggregation
    assert c.schema[c.schema_name].function_lists[1].name == "f"
    assert c.schema[c.schema_name].function_lists[1].parameters == [("x", str)]
    assert c.schema[c.schema_name].function_lists[1].return_type == str
    assert not c.schema[c.schema_name].function_lists[1].aggregation


def test_aggregation_adding():
    c = Context()

    assert not c.schema[c.schema_name].function_lists
    assert not c.schema[c.schema_name].functions

    f = lambda x: x
    c.register_aggregation(f, "f", [("x", int)], float)

    assert "f" in c.schema[c.schema_name].functions
    assert c.schema[c.schema_name].functions["f"] == f
    assert len(c.schema[c.schema_name].function_lists) == 2
    assert c.schema[c.schema_name].function_lists[0].name == "F"
    assert c.schema[c.schema_name].function_lists[0].parameters == [("x", int)]
    assert c.schema[c.schema_name].function_lists[0].return_type == float
    assert c.schema[c.schema_name].function_lists[0].aggregation
    assert c.schema[c.schema_name].function_lists[1].name == "f"
    assert c.schema[c.schema_name].function_lists[1].parameters == [("x", int)]
    assert c.schema[c.schema_name].function_lists[1].return_type == float
    assert c.schema[c.schema_name].function_lists[1].aggregation

    # Without replacement
    c.register_aggregation(f, "f", [("x", float)], int, replace=False)

    assert "f" in c.schema[c.schema_name].functions
    assert c.schema[c.schema_name].functions["f"] == f
    assert len(c.schema[c.schema_name].function_lists) == 4
    assert c.schema[c.schema_name].function_lists[2].name == "F"
    assert c.schema[c.schema_name].function_lists[2].parameters == [("x", float)]
    assert c.schema[c.schema_name].function_lists[2].return_type == int
    assert c.schema[c.schema_name].function_lists[2].aggregation
    assert c.schema[c.schema_name].function_lists[3].name == "f"
    assert c.schema[c.schema_name].function_lists[3].parameters == [("x", float)]
    assert c.schema[c.schema_name].function_lists[3].return_type == int
    assert c.schema[c.schema_name].function_lists[3].aggregation

    # With replacement
    f = lambda x: x + 1
    c.register_aggregation(f, "f", [("x", str)], str, replace=True)

    assert "f" in c.schema[c.schema_name].functions
    assert c.schema[c.schema_name].functions["f"] == f
    assert len(c.schema[c.schema_name].function_lists) == 2
    assert c.schema[c.schema_name].function_lists[0].name == "F"
    assert c.schema[c.schema_name].function_lists[0].parameters == [("x", str)]
    assert c.schema[c.schema_name].function_lists[0].return_type == str
    assert c.schema[c.schema_name].function_lists[0].aggregation
    assert c.schema[c.schema_name].function_lists[1].name == "f"
    assert c.schema[c.schema_name].function_lists[1].parameters == [("x", str)]
    assert c.schema[c.schema_name].function_lists[1].return_type == str
    assert c.schema[c.schema_name].function_lists[1].aggregation
