from __future__ import annotations

import itertools
import logging
from collections import defaultdict
from functools import lru_cache
from typing import List

import dask
import dask.dataframe as dd
import pyarrow.parquet as pq
from dask.dataframe.io.parquet.arrow import ArrowDatasetEngine
from dask.dataframe.io.parquet.core import ParquetFunctionWrapper
from dask.dataframe.io.utils import _is_local_fs
from dask.delayed import delayed
from dask.layers import DataFrameIOLayer
from dask.utils_test import hlg_layer

logger = logging.getLogger(__name__)


def parquet_statistics(
    ddf: dd.DataFrame,
    columns: List | None = None,
    task_size: int | None = None,
    **compute_kwargs,
) -> List[dict]:
    """Extract Parquet statistics from a Dask DataFrame collection

    WARNING: This API is experimental

    Parameters
    ----------
    ddf : dd.DataFrame
        Dask-DataFrame object to extract Parquet statistics from.
    columns : list or None, Optional
        List of columns to collect min/max statistics for. If ``None``
        (the default), only 'num-rows' statistics will be collected.
    task_size : int or None, Optional
        The number of distinct files to collect statistics for
        within each ``dask.delayed`` task. By default, this will
        be set to the total number of distinct paths on local
        filesystems, and 16 otherwise.
    **compute_kwargs : dict
        Key-word arguments to pass through to ``dask.compute``.

    Returns
    -------
    statistics : List[dict]
        List of Parquet statistics. Each list element corresponds
        to a distinct task (partition) in ``layer``. Each element
        of ``statistics`` will correspond to a dictionary with
        'num-rows' and 'columns' keys::

            ``{'num-rows': 1024, 'columns': [...]}``

        If column statistics are available, each element of the
        list stored under the "columns" key will correspond to
        a dictionary with "name", "min", and "max" keys::

            ``{'name': 'col0', 'min': 0, 'max': 100}``
    """

    # Check that we have a supported `ddf` object
    if not isinstance(ddf, dd.DataFrame):
        raise ValueError(f"Expected Dask DataFrame, got {type(ddf)}.")

    # Be strict about columns argument
    if columns:
        if not isinstance(columns, list):
            raise ValueError(f"Expected list, got columns={type(columns)}.")
        elif not set(columns).issubset(set(ddf.columns)):
            raise ValueError(f"columns={columns} must be a subset of {ddf.columns}")

    # Extract "read-parquet" layer from ddf
    layer = hlg_layer(ddf.dask, "read-parquet")

    # Make sure we are dealing with a
    # ParquetFunctionWrapper-based DataFrameIOLayer
    if not isinstance(layer, DataFrameIOLayer) or not isinstance(
        layer.io_func, ParquetFunctionWrapper
    ):
        logger.warning(
            f"Could not extract Parquet statistics from {ddf}."
            f"\nAttempted IO layer: {layer}"
        )
        return None

    # Collect statistics using layer information
    parts = layer.inputs
    fs = layer.io_func.fs
    engine = layer.io_func.engine
    if not isinstance(engine(), ArrowDatasetEngine):
        logger.warning(
            f"Could not extract Parquet statistics from {ddf}."
            f"\nUnsupported parquet engine: {engine}"
        )
        return None

    # Group parts corresponding to the same file.
    # A single task should always parse statistics
    # for all these parts at once (since they will
    # all be in the same footer)
    groups = defaultdict(list)
    for part in parts:
        path = part.get("piece")[0]
        groups[path].append(part)
    group_keys = list(groups.keys())

    # Set task_size according to fs type
    task_size = task_size or (len(groups) if _is_local_fs(fs) else 16)

    # Compute and return flattened result
    func = delayed(_read_partition_stats_group)
    result = dask.compute(
        [
            func(
                list(
                    itertools.chain(*[groups[k] for k in group_keys[i : i + task_size]])
                ),
                fs,
                engine,
                columns=columns,
            )
            for i in range(0, len(group_keys), task_size)
        ],
        **(compute_kwargs or {}),
    )[0]
    return list(itertools.chain(*result))


def _read_partition_stats_group(parts, fs, engine, columns=None):
    # Helper function used by _extract_statistics
    return [_read_partition_stats(part, fs, columns=columns) for part in parts]


def _read_partition_stats(part, fs, columns=None):
    # Herlper function to read Parquet-metadata
    # statistics for a single partition

    if not isinstance(part, list):
        part = [part]

    column_stats = {}
    num_rows = 0
    columns = columns or []
    for p in part:
        piece = p["piece"]
        path = piece[0]
        row_groups = None if piece[1] == [None] else piece[1]
        md = _get_md(path, fs)
        if row_groups is None:
            row_groups = list(range(md.num_row_groups))
        for rg in row_groups:
            row_group = md.row_group(rg)
            num_rows += row_group.num_rows
            for i in range(row_group.num_columns):
                col = row_group.column(i)
                name = col.path_in_schema
                if name in columns:
                    if col.statistics and col.statistics.has_min_max:
                        if name in column_stats:
                            column_stats[name]["min"] = min(
                                column_stats[name]["min"], col.statistics.min
                            )
                            column_stats[name]["max"] = max(
                                column_stats[name]["max"], col.statistics.max
                            )
                        else:
                            column_stats[name] = {
                                "min": col.statistics.min,
                                "max": col.statistics.max,
                            }

    # Convert dict-of-dict to list-of-dict to be consistent
    # with current `dd.read_parquet` convention (for now)
    column_stats_list = [
        {
            "name": name,
            "min": column_stats[name]["min"],
            "max": column_stats[name]["max"],
        }
        for name in column_stats.keys()
    ]
    return {"num-rows": num_rows, "columns": column_stats_list}


@lru_cache(maxsize=1)
def _get_md(path, fs):
    # Caching utility to avoid parsing the same footer
    # metadata multiple times
    with fs.open(path, default_cache="none") as f:
        return pq.ParquetFile(f).metadata
