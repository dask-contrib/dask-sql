from typing import Any

import dask.dataframe as dd

from dask_sql.input_utils.base import BaseInputPlugin


class DaskInputPlugin(BaseInputPlugin):
    """Input Plugin for Dask DataFrames, just keeping them"""

    def is_correct_input(
        self, input_item: Any, table_name: str, format: str = None, **kwargs
    ):
        return isinstance(input_item, dd.DataFrame) or format == "dask"

    def to_dc(self, input_item: Any, table_name: str, format: str = None, **kwargs):
        return input_item
