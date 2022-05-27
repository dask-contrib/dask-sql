import pandas as pd

from tests.utils import assert_eq


def test_analyze(c, df):
    result_df = c.sql("ANALYZE TABLE df COMPUTE STATISTICS FOR ALL COLUMNS")

    expected_df = pd.DataFrame(
        {
            "a": [
                700.0,
                df.a.mean(),
                df.a.std(),
                df.a.min(),
                # Dask's approx quantiles do not match up with pandas and must be specified explicitly
                2.0,
                2.0,
                3.0,
                df.a.max(),
                "double",
                "a",
            ],
            "b": [
                700.0,
                df.b.mean(),
                df.b.std(),
                df.b.min(),
                # Dask's approx quantiles do not match up with pandas and must be specified explicitly
                2.73108,
                5.20286,
                7.60595,
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

    assert_eq(result_df, expected_df)

    result_df = c.sql("ANALYZE TABLE df COMPUTE STATISTICS FOR COLUMNS a")

    assert_eq(result_df, expected_df[["a"]])
