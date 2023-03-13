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

from dask_sql.utils import make_pickable_without_dask_sql

logger = logging.getLogger(__name__)


def parquet_statistics(
    ddf: dd.DataFrame,
    columns: List | None = None,
    parallel: int | False | None = None,
    **compute_kwargs,
) -> List[dict] | None:
    """Extract Parquet statistics from a Dask DataFrame collection

    WARNING: This API is experimental

    Parameters
    ----------
    ddf
        Dask-DataFrame object to extract Parquet statistics from.
    columns
        List of columns to collect min/max statistics for. If ``None``
        (the default), only 'num-rows' statistics will be collected.
    parallel
        The number of distinct files to collect statistics for
        within a distinct ``dask.delayed`` task. If ``False``, all
        statistics will be parsed on the client process. If ``None``,
        the value will be set to 16 for remote filesystem (e.g s3)
        and ``False`` otherwise. Default is ``None``.
    **compute_kwargs
        Key-word arguments to pass through to ``dask.compute`` when
        ``parallel`` is not ``False``.

    Returns
    -------
    statistics
        List of Parquet statistics. Each list element corresponds
        to a distinct partition in ``ddf``. Each element of
        ``statistics`` will correspond to a dictionary with
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
            raise ValueError(f"Expected columns to be a list, got {type(columns)}.")
        elif not set(columns).issubset(set(ddf.columns)):
            raise ValueError(f"columns={columns} must be a subset of {ddf.columns}")

    # Extract "read-parquet" layer from ddf
    try:
        layer = hlg_layer(ddf.dask, "read-parquet")
    except KeyError:
        layer = None

    # Make sure we are dealing with a
    # ParquetFunctionWrapper-based DataFrameIOLayer
    if not isinstance(layer, DataFrameIOLayer) or not isinstance(
        layer.io_func, ParquetFunctionWrapper
    ):
        logger.debug(
            f"Could not extract Parquet statistics from {ddf}."
            f"\nAttempted IO layer: {layer}"
        )
        return None

    # Collect statistics using layer information
    parts = layer.inputs
    fs = layer.io_func.fs
    engine = layer.io_func.engine
    if not issubclass(engine, ArrowDatasetEngine):
        logger.debug(
            f"Could not extract Parquet statistics from {ddf}."
            f"\nUnsupported parquet engine: {engine}"
        )
        return None

    # Set default
    if parallel is None:
        parallel = False if _is_local_fs(fs) else 16
    parallel = int(parallel)

    if parallel:
        # Group parts corresponding to the same file.
        # A single task should always parse statistics
        # for all these parts at once (since they will
        # all be in the same footer)
        groups = defaultdict(list)
        for part in parts:
            for p in [part] if isinstance(part, dict) else part:
                path = p.get("piece")[0]
                groups[path].append(p)
        group_keys = list(groups.keys())

        # Compute and return flattened result
        func = delayed(_read_partition_stats_group)
        result = dask.compute(
            [
                func(
                    list(
                        itertools.chain(
                            *[groups[k] for k in group_keys[i : i + parallel]]
                        )
                    ),
                    fs,
                    engine,
                    columns=columns,
                )
                for i in range(0, len(group_keys), parallel)
            ],
            **(compute_kwargs or {}),
        )[0]
        return list(itertools.chain(*result))
    else:
        # Serial computation on client
        return _read_partition_stats_group(parts, fs, engine, columns=columns)


@make_pickable_without_dask_sql
def _read_partition_stats_group(parts, fs, engine, columns=None):
    def _read_partition_stats(part, fs, columns=None):
        # Helper function to read Parquet-metadata
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

    # Helper function used by _extract_statistics
    return [_read_partition_stats(part, fs, columns=columns) for part in parts]
