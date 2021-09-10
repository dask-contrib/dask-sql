import pytest

pytest.importorskip("dask_cudf")

from cudf.testing._utils import assert_eq


def test_cudf_order_by(c):
    df = c.sql(
        """
    SELECT
        *
    FROM cudf_user_table_1
    ORDER BY user_id
    """
    ).compute()

    expected_df = (
        c.sql(
            """
    SELECT
        *
    FROM cudf_user_table_1
    """
        )
        .sort_values(by="user_id", ignore_index=True)
        .compute()
    )

    assert_eq(df, expected_df)
