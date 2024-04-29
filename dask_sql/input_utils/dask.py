from typing import Any

import dask.dataframe as dd

from dask_sql.input_utils.base import BaseInputPlugin


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
            try:
                import dask_cudf  # noqa: F401
            except ImportError:
                raise ModuleNotFoundError(
                    "Setting `gpu=True` for table creation requires dask_cudf"
                )
            return input_item.to_backend("cudf", **kwargs)
        return input_item
