import dask.dataframe as dd
import pandas as pd
import pytest


@pytest.mark.skip(
    reason="WIP DataFusion - https://github.com/dask-contrib/dask-sql/issues/537"
)
@pytest.mark.parametrize("gpu", [False, pytest.param(True, marks=pytest.mark.gpu)])
def test_distribute_by(c, gpu):
    df = pd.DataFrame({"id": [0, 1, 2, 1, 2, 3], "val": [0, 1, 2, 1, 2, 3]})
    ddf = dd.from_pandas(df, npartitions=2)

    c.create_table("test", ddf, gpu=gpu)
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

    if gpu:
        part_0_ids = part_0_ids.to_pandas()
        part_1_ids = part_1_ids.to_pandas()

    assert bool(set(part_0_ids) & set(part_1_ids)) is False
