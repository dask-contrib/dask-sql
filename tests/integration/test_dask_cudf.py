import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

pytest.importorskip("dask_cudf")


def test_cudf_order_by(c):
    df = c.sql(
        """
    SELECT
        *
    FROM cudf_user_table_1
    ORDER BY user_id
    """
    )
    df = df.compute().to_pandas()

    expected_df = pd.DataFrame(
        {"user_id": [2, 1, 2, 3], "b": [3, 3, 1, 3]}
    ).sort_values(by="user_id")
    assert_frame_equal(df, expected_df)
