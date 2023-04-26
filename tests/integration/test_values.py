import pandas as pd
import pytest

from tests.utils import assert_eq


def test_values(c):
    result_df = c.sql(
        """
        SELECT * FROM (VALUES (1, 2), (1, 3)) as tbl(column1, column2)
        """
    )
    expected_df = pd.DataFrame({"column1": [1, 1], "column2": [2, 3]})
    assert_eq(result_df, expected_df, check_index=False)


def test_values_join(c):
    result_df = c.sql(
        """
        SELECT * FROM df_simple, (VALUES (1, 2), (1, 3)) as tbl(aa, bb)
        WHERE a = aa
        """
    )
    expected_df = pd.DataFrame(
        {"a": [1, 1], "b": [1.1, 1.1], "aa": [1, 1], "bb": [2, 3]}
    )
    assert_eq(result_df, expected_df, check_index=False)


@pytest.mark.xfail(reason="Datafusion doesn't handle values relations cleanly")
def test_values_join_alias(c):
    result_df = c.sql(
        """
        SELECT * FROM df_simple, (VALUES (1, 2), (1, 3)) as tbl(aa, bb)
        WHERE a = tbl.aa
        """
    )
    expected_df = pd.DataFrame(
        {"a": [1, 1], "b": [1.1, 1.1], "aa": [1, 1], "bb": [2, 3]}
    )
    assert_eq(result_df, expected_df, check_index=False)

    result_df = c.sql(
        """
        SELECT * FROM df_simple t1, (VALUES (1, 2), (1, 3)) as t2(a, b)
        WHERE t1.a = t2.a
        """
    )
    expected_df = pd.DataFrame(
        {"t1.a": [1, 1], "t1.b": [1.1, 1.1], "t2.aa": [1, 1], "t2.bb": [2, 3]}
    )
    assert_eq(result_df, expected_df, check_index=False)
