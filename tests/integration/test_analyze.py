import pandas as pd
import pytest

from tests.utils import assert_eq


@pytest.mark.skip(reason="WIP Datafusion")
def test_analyze(c, df):
    result_df = c.sql("ANALYZE TABLE df COMPUTE STATISTICS FOR ALL COLUMNS")

    expected_df = pd.DataFrame(
        {
            "a": [
                700.0,
                df.a.mean(),
                df.a.std(),
                1.0,
                2.0,
                2.0,  # incorrect, but what Dask gives for approx quantile
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
    assert_eq(result_df, expected_df, rtol=0.135)

    result_df = c.sql("ANALYZE TABLE df COMPUTE STATISTICS FOR COLUMNS a")

    assert_eq(result_df, expected_df[["a"]], rtol=0.135)
