import dask.dataframe as dd
import pandas as pd

from dask_sql.mappings import python_to_sql_type
from tests.utils import assert_eq


def test_analyze(c, df):
    result_df = c.sql("ANALYZE TABLE df COMPUTE STATISTICS FOR ALL COLUMNS")

    # extract table and compute stats with Dask manually
    expected_df = dd.concat(
        [
            c.sql("SELECT * FROM df").describe(),
            pd.DataFrame(
                {
                    col: str(python_to_sql_type(df[col].dtype)).lower()
                    for col in df.columns
                },
                index=["data_type"],
            ),
            pd.DataFrame(
                {col: col for col in df.columns},
                index=["col_name"],
            ),
        ]
    )

    assert_eq(result_df, expected_df)

    result_df = c.sql("ANALYZE TABLE df COMPUTE STATISTICS FOR COLUMNS a")

    assert_eq(result_df, expected_df[["a"]])
