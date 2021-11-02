import dask.dataframe as dd
import pandas as pd


def test_distribute_by(c):
    df_0 = pd.DataFrame({"id": [0, 1, 2], "val": [0, 1, 2]})

    df_1 = pd.DataFrame({"id": [1, 2, 3], "val": [1, 2, 3]})

    ddf_0 = dd.from_pandas(df_0, npartitions=1)
    ddf_1 = dd.from_pandas(df_1, npartitions=1)
    ddf = dd.concat([ddf_0, ddf_1])

    partitioned_ddf = ddf.shuffle(on=["id"])
    part_0_ids = partitioned_ddf.get_partition(0).compute().id.unique()
    part_1_ids = partitioned_ddf.get_partition(1).compute().id.unique()
    assert bool(set(part_0_ids) & set(part_1_ids)) is False

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
