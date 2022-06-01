import pandas as pd
import pytest

from tests.utils import assert_eq


def test_union_not_all(c, df):
    result_df = c.sql(
        """
        SELECT * FROM df
        UNION
        SELECT * FROM df
        UNION
        SELECT * FROM df
        """
    )

    assert_eq(result_df, df, check_index=False)


def test_union_all(c, df):
    result_df = c.sql(
        """
        SELECT * FROM df
        UNION ALL
        SELECT * FROM df
        UNION ALL
        SELECT * FROM df
        """
    )
    expected_df = pd.concat([df, df, df], ignore_index=True)

    assert_eq(result_df, expected_df, check_index=False)


@pytest.mark.skip(reason="WIP DataFusion")
def test_union_mixed(c, df, long_table):
    result_df = c.sql(
        """
        SELECT a AS "I", b as "II" FROM df
        UNION ALL
        SELECT a as "I", a as "II" FROM long_table
        """
    )
    long_table = long_table.rename(columns={"a": "I"})
    long_table["II"] = long_table["I"]
    expected_df = pd.concat(
        [df.rename(columns={"a": "I", "b": "II"}), long_table],
        ignore_index=True,
    )

    assert_eq(result_df, expected_df, check_index=False)
