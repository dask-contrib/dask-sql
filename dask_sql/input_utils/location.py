import os
from typing import Any

import dask.dataframe as dd
from distributed.client import default_client

from dask_sql.input_utils.base import BaseInputPlugin


class LocationInputPlugin(BaseInputPlugin):
    """Input Plugin for everything, which can be read in from a file (on disk, remote etc.)"""

    def is_correct_input(
        self, input_item: Any, table_name: str, format: str = None, **kwargs
    ):
        return isinstance(input_item, str)

    def to_dc(self, input_item: Any, table_name: str, format: str = None, **kwargs):

        if format == "memory":
            client = default_client()
            return client.get_dataset(input_item, **kwargs)

        if not format:
            _, extension = os.path.splitext(input_item)

            format = extension.lstrip(".")

        try:
            read_function = getattr(dd, f"read_{format}")
        except AttributeError:
            raise AttributeError(f"Can not read files of format {format}")

        return read_function(input_item, **kwargs)
