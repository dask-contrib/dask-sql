from typing import Any, Callable

import dask
import dask.dataframe as dd


def map_on_partition_index(
    df: dd.DataFrame, f: Callable, *args: Any, **kwargs: Any
) -> dd.DataFrame:
    return dd.from_delayed(
        [
            dask.delayed(f)(partition, partition_number, *args, **kwargs)
            for partition_number, partition in enumerate(df.partitions)
        ]
    )
