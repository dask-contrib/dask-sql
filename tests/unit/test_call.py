from unittest.mock import MagicMock
import operator

import dask.dataframe as dd
import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal

import dask_sql.physical.rex.core.call as call

df1 = dd.from_pandas(pd.DataFrame({"a": [1, 2, 3]}), npartitions=1)
df2 = dd.from_pandas(pd.DataFrame({"a": [3, 2, 1]}), npartitions=1)
df3 = dd.from_pandas(
    pd.DataFrame({"a": [True, pd.NA, False]}, dtype="boolean"), npartitions=1
)
ops_mapping = call.RexCallPlugin.OPERATION_MAPPING


def test_operation():
    operator = MagicMock()
    operator.return_value = "test"

    op = call.Operation(operator)

    assert op("input") == "test"
    operator.assert_called_once_with("input")


def test_reduce():
    op = call.ReduceOperation(operator.add)

    assert op(1, 2, 3) == 6


def test_case():
    op = call.CaseOperation()

    assert_series_equal(
        op(df1.a > 2, df1.a, df2.a).compute(), pd.Series([3, 2, 3]), check_names=False
    )

    assert_series_equal(
        op(df1.a > 2, 99, df2.a).compute(), pd.Series([3, 2, 99]), check_names=False
    )

    assert_series_equal(
        op(df1.a > 2, 99, -1).compute(), pd.Series([-1, -1, 99]), check_names=False
    )

    assert_series_equal(
        op(df1.a > 2, df1.a, -1).compute(), pd.Series([-1, -1, 3]), check_names=False
    )

    assert op(True, 1, 2) == 1
    assert op(False, 1, 2) == 2


def test_is_true():
    op = call.IsTrueOperation()

    assert_series_equal(
        op(df1.a > 2).compute(), pd.Series([False, False, True]), check_names=False
    )
    assert_series_equal(
        op(df3.a).compute(),
        pd.Series([True, False, False], dtype="boolean"),
        check_names=False,
    )

    assert op(1) == True
    assert op(0) == False
    assert op(None) == False
    assert op(np.NaN) == False
    assert op(pd.NA) == False


def test_is_false():
    op = call.IsFalseOperation()

    assert_series_equal(
        op(df1.a > 2).compute(), pd.Series([True, True, False]), check_names=False
    )
    assert_series_equal(
        op(df3.a).compute(),
        pd.Series([False, False, True], dtype="boolean"),
        check_names=False,
    )

    assert op(1) == False
    assert op(0) == True
    assert op(None) == False
    assert op(np.NaN) == False
    assert op(pd.NA) == False


def test_like():
    op = call.LikeOperation()

    assert op("a string", r"%a%") == True
    assert op("another string", r"a%") == True
    assert op("another string", r"s%") == False
    assert op("normal", r"n[a-z]rm_l") == True
    assert op("not normal", r"n[a-z]rm_l") == False


def test_nan():
    op = call.IsNullOperation()

    assert op(None) == True
    assert op(np.NaN) == True
    assert op(pd.NA) == True
    assert_series_equal(
        op(pd.Series(["a", None, "c"])), pd.Series([False, True, False])
    )
    assert_series_equal(
        op(pd.Series([3, 2, np.NaN, pd.NA])), pd.Series([False, False, True, True])
    )


def test_simple_ops():
    assert_series_equal(
        ops_mapping["and"](df1.a >= 2, df2.a >= 2).compute(),
        pd.Series([False, True, False]),
        check_names=False,
    )

    assert_series_equal(
        ops_mapping["or"](df1.a >= 2, df2.a >= 2).compute(),
        pd.Series([True, True, True]),
        check_names=False,
    )

    assert_series_equal(
        ops_mapping[">="](df1.a, df2.a).compute(),
        pd.Series([False, True, True]),
        check_names=False,
    )

    assert_series_equal(
        ops_mapping["+"](df1.a, df2.a, df1.a).compute(),
        pd.Series([5, 6, 7]),
        check_names=False,
    )


def test_math_operations():
    assert_series_equal(
        ops_mapping["abs"](-df1.a).compute(), pd.Series([1, 2, 3]), check_names=False,
    )
    assert_series_equal(
        ops_mapping["round"](df1.a).compute(), pd.Series([1, 2, 3]), check_names=False,
    )
    assert_series_equal(
        ops_mapping["floor"](df1.a).compute(),
        pd.Series([1.0, 2.0, 3.0]),
        check_names=False,
    )

    assert ops_mapping["abs"](-5) == 5
    assert ops_mapping["round"](1.234, 2) == 1.23
    assert ops_mapping["floor"](1.234) == 1


def test_string_operations():
    a = "a normal string"
    assert ops_mapping["char_length"](a) == 15
    assert ops_mapping["upper"](a) == "A NORMAL STRING"
    assert ops_mapping["lower"](a) == "a normal string"
    assert ops_mapping["position"]("a", a, 4) == 7
    assert ops_mapping["position"]("ZL", a) == 0
    assert ops_mapping["trim"]("BOTH", "a", a) == " normal string"
    assert ops_mapping["trim"]("LEADING", "a", a) == " normal string"
    assert ops_mapping["trim"]("TRAILING", "a", a) == "a normal string"
    assert ops_mapping["overlay"](a, "XXX", 2) == "aXXXrmal string"
    assert ops_mapping["overlay"](a, "XXX", 2, 4) == "aXXXmal string"
    assert ops_mapping["overlay"](a, "XXX", 2, 1) == "aXXXnormal string"
    assert ops_mapping["substring"](a, -1) == "a normal string"
    assert ops_mapping["substring"](a, 10) == "string"
    assert ops_mapping["substring"](a, 2) == " normal string"
    assert ops_mapping["substring"](a, 2, 2) == " n"
    assert ops_mapping["initcap"](a) == "A Normal String"
