import pytest

import pandas as pd
from pandas.testing import assert_frame_equal


def test_analyze(c, df):
    result_df = c.sql("ANALYZE TABLE df COMPUTE STATISTICS FOR ALL COLUMNS")
    result_df = result_df.compute()

    expected_df = pd.DataFrame(
        {
            "a": [
                700.0,
                df.a.mean(),
                df.a.std(),
                1.0,
                2.0,
                3.0,
                3.0,
                3.0,
                "double",
                "a",
            ],
            "b": [
                700.0,
                df.b.mean(),
                df.b.std(),
                df.b.min(),
                df.b.quantile(0.25),
                df.b.quantile(0.5),
                df.b.quantile(0.75),
                df.b.max(),
                "double",
                "b",
            ],
        },
        index=[
            "count",
            "mean",
            "std",
            "min",
            "25%",
            "50%",
            "75%",
            "max",
            "data_type",
            "col_name",
        ],
    )

    # The percentiles are calculated only approximately, therefore we do not use exact matching
    assert_frame_equal(result_df, expected_df, check_exact=False, atol=1)

    result_df = c.sql("ANALYZE TABLE df COMPUTE STATISTICS FOR COLUMNS a")
    result_df = result_df.compute()

    assert_frame_equal(result_df, expected_df[["a"]])
