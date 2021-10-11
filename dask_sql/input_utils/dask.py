from typing import Any

import dask.dataframe as dd

from dask_sql.input_utils.base import BaseInputPlugin

try:
    import dask_cudf
except ImportError:
    dask_cudf = None


class DaskInputPlugin(BaseInputPlugin):
    """Input Plugin for Dask DataFrames, just keeping them"""

    def is_correct_input(
        self, input_item: Any, table_name: str, format: str = None, **kwargs
    ):
        return isinstance(input_item, dd.DataFrame) or format == "dask"

    def to_dc(
        self,
        input_item: Any,
        table_name: str,
        format: str = None,
        gpu: bool = False,
        **kwargs
    ):
        if gpu:  # pragma: no cover
            if not dask_cudf:
                raise ModuleNotFoundError(
                    "Setting `gpu=True` for table creation requires dask_cudf"
                )
            if not isinstance(input_item, dask_cudf.DataFrame):
                input_item = dask_cudf.from_dask_dataframe(input_item, **kwargs)
        return input_item
