import logging
from typing import TYPE_CHECKING, Union

import dask.dataframe as dd
import pandas as pd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.input_utils.base import BaseInputPlugin
from dask_sql.utils import Pluggable

if TYPE_CHECKING:
    import cudf
    import hive
    import sqlalchemy

logger = logging.Logger(__name__)

InputType = Union[
    dd.DataFrame,
    pd.DataFrame,
    str,
    Union[
        "sqlalchemy.engine.base.Connection",
        "hive.Cursor",
        "cudf.core.dataframe.DataFrame",
    ],
]


class InputUtil(Pluggable):
    """
    Plugin list and helper class for transforming the inputs to
    create table into a dask dataframe
    """

    @classmethod
    def add_plugin_class(cls, plugin_class: BaseInputPlugin, replace=True):
        """Convenience function to add a class directly to the plugins"""
        logger.debug(f"Registering Input plugin for {plugin_class}")
        cls.add_plugin(str(plugin_class), plugin_class(), replace=replace)

    @classmethod
    def to_dc(
        cls,
        input_item: InputType,
        table_name: str,
        format: str = None,
        persist: bool = True,
        gpu: bool = False,
        **kwargs,
    ) -> DataContainer:
        """
        Turn possible input descriptions or formats (e.g. dask dataframes, pandas dataframes,
        locations as string, hive tables) into the loaded data containers,
        maybe persist them to cluster memory before.
        """
        filled_get_dask_dataframe = lambda *args: cls._get_dask_dataframe(
            *args,
            table_name=table_name,
            format=format,
            gpu=gpu,
            **kwargs,
        )

        if isinstance(input_item, list):
            table = dd.concat([filled_get_dask_dataframe(item) for item in input_item])
        else:
            table = filled_get_dask_dataframe(input_item)

        if persist:
            table = table.persist()

        return DataContainer(table.copy(), ColumnContainer(table.columns))

    @classmethod
    def _get_dask_dataframe(
        cls,
        input_item: InputType,
        table_name: str,
        format: str = None,
        gpu: bool = False,
        **kwargs,
    ):
        plugin_list = cls.get_plugins()

        for plugin in plugin_list:
            if plugin.is_correct_input(
                input_item, table_name=table_name, format=format, **kwargs
            ):
                return plugin.to_dc(
                    input_item, table_name=table_name, format=format, gpu=gpu, **kwargs
                )

        raise ValueError(f"Do not understand the input type {type(input_item)}")
