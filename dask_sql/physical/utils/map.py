from typing import Any, Callable

import dask
import dask.dataframe as dd
from nvtx import annotate


@annotate("MAP_MAP_ON_PARTITION_INDEX", color="green", domain="dask_sql_python")
def map_on_partition_index(
    df: dd.DataFrame, f: Callable, *args: Any, **kwargs: Any
) -> dd.DataFrame:
    return dd.from_delayed(
        [
            dask.delayed(f)(partition, partition_number, *args, **kwargs)
            for partition_number, partition in enumerate(df.partitions)
        ]
    )
