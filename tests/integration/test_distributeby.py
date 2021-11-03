import dask.dataframe as dd
import pandas as pd
import pytest

try:
    import cudf
    import dask_cudf
except ImportError:
    cudf = None
    dask_cudf = None


@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_distribute_by(c, gpu):
    df = pd.DataFrame({"id": [0, 1, 2, 1, 2, 3], "val": [0, 1, 2, 1, 2, 3]})

    if gpu:
        gdf = cudf.from_pandas(df)
        ddf = dask_cudf.from_cudf(df, npartitions=2)
    else:
        ddf = dd.from_pandas(df, npartitions=2)

    c.create_table("test", ddf)
    partitioned_ddf = c.sql(
        """
    SELECT
    id
    FROM test
    DISTRIBUTE BY id
    """
    )
    part_0_ids = partitioned_ddf.get_partition(0).compute().id.unique()
    part_1_ids = partitioned_ddf.get_partition(1).compute().id.unique()
    assert bool(set(part_0_ids) & set(part_1_ids)) is False
