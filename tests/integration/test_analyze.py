import pandas as pd

from dask_sql.mappings import python_to_sql_type
from tests.utils import assert_eq, xfail_on_gpu


def test_analyze(request, c, df):
    xfail_on_gpu(request, c, "Can't create mixed datatype columns on cuDF")

    result_df = c.sql("ANALYZE TABLE df COMPUTE STATISTICS FOR ALL COLUMNS")

    # extract table and compute stats with Dask manually
    expected_df = (
        c.sql("SELECT * FROM df")
        .describe()
        .append(
            pd.Series(
                {
                    col: str(python_to_sql_type(df[col].dtype)).lower()
                    for col in df.columns
                },
                name="data_type",
            )
        )
        .append(
            pd.Series(
                {col: col for col in df.columns},
                name="col_name",
            )
        )
    )

    assert_eq(result_df, expected_df)

    result_df = c.sql("ANALYZE TABLE df COMPUTE STATISTICS FOR COLUMNS a")

    assert_eq(result_df, expected_df[["a"]])
