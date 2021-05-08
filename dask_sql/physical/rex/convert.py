import logging
from typing import List, Tuple

from dask_sql.datacontainer import DataContainer
from dask_sql.java import get_java_class
from dask_sql.physical.rex.base import (
    BaseRexPlugin,
    ColumnReference,
    OutputColumn,
    ScalarValue,
    SeriesOrScalar,
)
from dask_sql.utils import LoggableDataFrame, Pluggable, new_temporary_column

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

        Best is to use the shortcut function "convert_and_get_list"
        in this class.

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

    @classmethod
    def convert_and_get_list(
        cls,
        operands: List["org.apache.calcite.rex.RexNode"],
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> Tuple[SeriesOrScalar, DataContainer]:
        """
        Convert a list of RexNodes and turn them into a list of
        dd.Series or Scalars.

        As RexPlugins are allowed to change the dataframes,
        extra care needs to be taken when converting
        multiple RexNodes, as columns referencing the original
        dataframe might be outdated (aka misaligned, as the divisions
        might have changed in between). This convenience
        function handles this properly by dereferencing
        the columns only at the end.
        """
        column_references = []
        for o in operands:
            column_reference, dc = cls.convert(o, dc, context=context)

            column_references.append(column_reference)

        return [c.get(dc) for c in column_references], dc

    @classmethod
    def convert_to_column_reference(
        cls,
        rex: "org.apache.calcite.rex.RexNode",
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> Tuple[ColumnReference, DataContainer]:
        """
        Convert the RexNode and always add the new column.

        RexPlugins can return either a reference to a new column
        (by first adding the new column and then returning a column reference,
        which is basically the column name) or a scalar value.
        Some methods however, need to have the calculation result be added to
        the dataframe. This function therefore assigns any scalar values
        directly to the dataframe and also returns a reference to the new column.

        Note: We do not add the newly created column to the
        DataContainer's column container, as the decision whether this is
        a temporary column or a frontend column needs to happen
        somewhere else.
        """
        new_column, dc = cls.convert(rex, dc, context=context)

        if isinstance(new_column, ColumnReference):
            return new_column, dc

        # This is a rare case where we actually want to turn a
        # scalar value into a full column, so we do
        # this manually here (and not in the corresponding plugin)
        df = dc.df
        cc = dc.column_container

        backend_column_name = new_temporary_column(df)
        df = df.assign(**{backend_column_name: new_column.get()})

        # We have updated the dataframe, so create a new datacontainer also
        dc = DataContainer(df, cc)
        return ColumnReference(backend_column_name), dc
