import pandas as pd

from tests.utils import assert_eq


def test_usertable_substrait_join(c):
    return_df = c.sql("./tests/integration/proto/df_simple.proto", substrait=True)
    expected_df = pd.DataFrame(
        {"user_id": [1, 1, 2, 2], "b": [3, 3, 1, 3], "c": [1, 2, 3, 3]}
    )

    assert_eq(return_df, expected_df, check_index=False)
