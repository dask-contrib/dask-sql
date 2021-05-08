import logging
from typing import Tuple

from dask_sql.datacontainer import DataContainer
from dask_sql.java import get_java_class
from dask_sql.physical.rex.base import BaseRexPlugin, OutputColumn
from dask_sql.utils import LoggableDataFrame, Pluggable

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
    ) -> Tuple[OutputColumn, DataContainer]:
        """
        Convert the given rel (java instance)
        into a python expression (a dask dataframe)
        using the stored plugins and the dictionary of
        registered dask tables.

        The result of this function consists of
        the calculation result (in form of a reference to a
        newly added column or as a scalar value) and
        the DataContainer. Plugins are allowed to change
        the datacontainer if needed (e.g. reorder/shuffle the rows).

        Important: do not return columns "as is", but only as references
        (column names). If you are adding new columns, make sure not
        to overwrite previous ones.

        Note: A common usage of these plugins, is to use them on
        a list of input operands. Make sure to resolve the references
        to the resulting columns only at the end of the list iteration!
        Intermediate steps might change the datacontainer, so your
        references might be invalidated.
        Example, do not do this:

            for o in operands:
                result, _ = RexConverter.convert(o, dc, ctx)
                # Reference resolved here, but dc might change
                # in later iterations!
                yield result.get(dc)

        but rather this

            results = []
            for o in operands:
                result, dc = RexConverter.convert(o, dc, ctx)
                results.append(result)

            # Reference resolved in the end, after all
            # operations changing dc are done
            yield from [r.get(dc) for r in results]

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

        column, dc = plugin_instance.convert(rex, dc, context=context)
        logger.debug(
            f"Processed REX {rex} into {LoggableDataFrame(column)} of {LoggableDataFrame(dc)}"
        )
        return column, dc
