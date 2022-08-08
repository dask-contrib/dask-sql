import pandas as pd
import pytest

from dask_sql.mappings import python_to_sql_type
from tests.utils import assert_eq


@pytest.mark.skip(reason="WIP DataFusion")
def test_analyze(c, df):
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
