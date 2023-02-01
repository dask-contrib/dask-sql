import dask.dataframe as dd
import pandas as pd
import pytest

from dask_sql.physical.utils.statistics import parquet_statistics


@pytest.mark.parametrize("parallel", [None, False, 2])
def test_parquet_statistics(parquet_ddf, parallel):

    # Check simple num-rows statistics
    stats = parquet_statistics(parquet_ddf, parallel=parallel)
    stats_df = pd.DataFrame(stats)
    num_rows = stats_df["num-rows"].sum()
    assert len(stats_df) == parquet_ddf.npartitions
    assert num_rows == len(parquet_ddf)

    # Check simple column statistics
    stats = parquet_statistics(parquet_ddf, columns=["b"], parallel=parallel)
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


def test_parquet_statistics_bad_args(parquet_ddf):
    # Check "bad" input arguments to parquet_statistics

    # ddf argument must be a Dask-DataFrame object
    pdf = pd.DataFrame({"a": range(10)})
    with pytest.raises(ValueError, match="Expected Dask DataFrame"):
        parquet_statistics(pdf)

    # Return should be None if parquet statistics
    # cannot be extracted from the provided collection
    ddf = dd.from_pandas(pdf, npartitions=2)
    assert parquet_statistics(ddf) is None

    # Clear error should be raised when columns is not
    # a list containing a subset of columns from ddf
    with pytest.raises(ValueError, match="Expected columns to be a list"):
        parquet_statistics(parquet_ddf, columns="bad")

    with pytest.raises(ValueError, match="must be a subset"):
        parquet_statistics(parquet_ddf, columns=["bad"])
