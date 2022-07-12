import logging
from typing import TYPE_CHECKING

import dask.dataframe as dd

from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.utils import LoggableDataFrame, Pluggable

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan

logger = logging.getLogger(__name__)


class RelConverter(Pluggable):
    """
    Helper to convert from rel to a python expression

    This class stores plugins which can convert from RelNodes to
    python expression (typically dask dataframes).
    The stored plugins are assumed to have a class attribute "class_name"
    to control, which java classes they can convert
    and they are expected to have a convert (instance) method
    in the form

        def convert(self, rel, context)

    to do the actual conversion.
    """

    @classmethod
    def add_plugin_class(cls, plugin_class: BaseRelPlugin, replace=True):
        """Convenience function to add a class directly to the plugins"""
        logger.debug(f"Registering REL plugin for {plugin_class.class_name}")
        cls.add_plugin(plugin_class.class_name, plugin_class(), replace=replace)

    @classmethod
    def convert(cls, rel: "LogicalPlan", context: "dask_sql.Context") -> dd.DataFrame:
        """
        Convert SQL AST tree node(s)
        into a python expression (a dask dataframe)
        using the stored plugins and the dictionary of
        registered dask tables from the context.
        The SQL AST tree is traversed. The context of the traversal is saved
        in the Rust logic. We need to take that current node and determine
        what "type" of Relational operator it represents to build the execution chain.
        """

        node_type = rel.get_current_node_type()

        try:
            plugin_instance = cls.get_plugin(node_type)
        except KeyError:  # pragma: no cover
            raise NotImplementedError(
                f"No relational conversion for node type {node_type} available (yet)."
            )
        logger.debug(
            f"Processing REL {rel} using {plugin_instance.__class__.__name__}..."
        )
        df = plugin_instance.convert(rel, context=context)
        logger.debug(f"Processed REL {rel} into {LoggableDataFrame(df)}")
        return df
