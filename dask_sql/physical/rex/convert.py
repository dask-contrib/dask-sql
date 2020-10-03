from typing import Union, Any
import logging

import dask.dataframe as dd

from dask_sql.java import get_java_class
from dask_sql.utils import LoggableDataFrame, Pluggable
from dask_sql.physical.rex.base import BaseRexPlugin
from dask_sql.datacontainer import DataContainer

logger = logging.getLogger(__name__)


class RexConverter(Pluggable):
    """
    Helper to convert from rex to a python expression

    This class stores plugins which can convert from RexNodes to
    python expression (single values or dask dataframe columns).
    The stored plugins are assumed to have a class attribute "class_name"
    to control, which java classes they can convert
    and they are expected to have a convert (instance) method
    in the form

        def convert(self, rex, df)

    to do the actual conversion.
    """

    @classmethod
    def add_plugin_class(cls, plugin_class: BaseRexPlugin, replace=True):
        """Convenience function to add a class directly to the plugins"""
        logger.debug(f"Registering REX plugin for {plugin_class.class_name}")
        cls.add_plugin(plugin_class.class_name, plugin_class(), replace=replace)

    @classmethod
    def convert(
        cls,
        rex: "org.apache.calcite.rex.RexNode",
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> Union[dd.DataFrame, Any]:
        """
        Convert the given rel (java instance)
        into a python expression (a dask dataframe)
        using the stored plugins and the dictionary of
        registered dask tables.
        """
        class_name = get_java_class(rex)

        try:
            plugin_instance = cls.get_plugin(class_name)
        except KeyError:  # pragma: no cover
            raise NotImplementedError(
                f"No conversion for class {class_name} available (yet)."
            )

        logger.debug(
            f"Processing REX {rex} using {plugin_instance.__class__.__name__}..."
        )

        df = plugin_instance.convert(rex, dc, context=context)
        logger.debug(f"Processed REX {rex} into {LoggableDataFrame(df)}")
        return df
