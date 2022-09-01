import os
from typing import Any

import dask.dataframe as dd
from distributed.client import default_client

from dask_sql.input_utils.base import BaseInputPlugin
from dask_sql.input_utils.convert import InputUtil


class LocationInputPlugin(BaseInputPlugin):
    """Input Plugin for everything, which can be read in from a file (on disk, remote etc.)"""

    def is_correct_input(
        self, input_item: Any, table_name: str, format: str = None, **kwargs
    ):
        return isinstance(input_item, str)

    def to_dc(
        self,
        input_item: Any,
        table_name: str,
        format: str = None,
        gpu: bool = False,
        **kwargs,
    ):
        if format == "memory":
            client = default_client()
            df = client.get_dataset(input_item, **kwargs)

            plugin_list = InputUtil.get_plugins()

            for plugin in plugin_list:
                if plugin.is_correct_input(df, table_name, format, **kwargs):
                    return plugin.to_dc(df, table_name, format, gpu, **kwargs)
        if not format:
            _, extension = os.path.splitext(input_item)

            format = extension.lstrip(".")
        try:
            if gpu:  # pragma: no cover
                try:
                    import dask_cudf
                except ImportError:
                    raise ModuleNotFoundError(
                        "Setting `gpu=True` for table creation requires dask-cudf"
                    )
                read_function = getattr(dask_cudf, f"read_{format}")
            else:
                read_function = getattr(dd, f"read_{format}")
        except AttributeError:
            raise AttributeError(f"Can not read files of format {format}")

        return read_function(input_item, **kwargs)
