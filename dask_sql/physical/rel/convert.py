from typing import Dict

import dask.dataframe as dd

from dask_sql.java import get_java_class
from dask_sql.utils import Pluggable
from dask_sql.physical.rel.base import BaseRelPlugin


class RelConverter(Pluggable):
    """
    Helper to convert from rel to a python expression

    This class stores plugins which can convert from RelNodes to
    python expression (typically dask dataframes).
    The stored plugins are assumed to have a class attribute "class_name"
    to control, which java classes they can convert
    and they are expected to have a convert (instance) method
    in the form

        def convert(self, rel, tables)

    to do the actual conversion.
    """

    @classmethod
    def add_plugin_class(cls, plugin_class: BaseRelPlugin, replace=True):
        """Convenience function to add a class directly to the plugins"""
        cls.add_plugin(plugin_class.class_name, plugin_class(), replace=replace)

    @classmethod
    def convert(
        cls, rel: "org.apache.calcite.rel.RelNode", tables: Dict[str, dd.DataFrame]
    ) -> dd.DataFrame:
        """
        Convert the given rel (java instance)
        into a python expression (a dask dataframe)
        using the stored plugins and the dictionary of
        registered dask tables.
        """
        class_name = get_java_class(rel)

        try:
            plugin_instace = cls.get_plugin(class_name)
        except KeyError:  # pragma: no cover
            raise NotImplementedError(
                f"No conversion for class {class_name} available (yet)."
            )

        df = plugin_instace.convert(rel, tables=tables)
        return df
