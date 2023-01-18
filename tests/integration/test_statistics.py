import pandas as pd

from dask_sql.physical.utils.statistics import parquet_statistics


def test_parquet_statistics(c, parquet_ddf):

    # Check simple num-rows statistics
    stats = parquet_statistics(parquet_ddf)
    stats_df = pd.DataFrame(stats)
    num_rows = stats_df["num-rows"].sum()
    assert len(stats_df) == parquet_ddf.npartitions
    assert num_rows == len(parquet_ddf)

    # Check simple column statistics
    stats = parquet_statistics(parquet_ddf, columns=["b"])
    b_stats = [
        {
            "min": stat["columns"][0]["min"],
            "max": stat["columns"][0]["max"],
        }
        for stat in stats
    ]
    b_stats_df = pd.DataFrame(b_stats)
    assert b_stats_df["min"].min() == parquet_ddf["b"].min().compute()
    assert b_stats_df["max"].max() == parquet_ddf["b"].max().compute()
