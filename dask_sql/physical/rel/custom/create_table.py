import logging
from typing import TYPE_CHECKING

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.utils import convert_sql_kwargs

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan

logger = logging.getLogger(__name__)


class CreateTablePlugin(BaseRelPlugin):
    """
    Create a table with given parameters from already existing data
    and register it at the context.
    The SQL call looks like

        CREATE TABLE <table-name> WITH (
            parameter = value,
            ...
        )

    It uses calls to "dask.dataframe.read_<format>"
    where format is given by the "format" parameter (defaults to CSV).
    The only mandatory parameter is the "location" parameter.

    Using this SQL is equivalent to just doing

        df = dd.read_<format>(location, **kwargs)
        context.register_dask_dataframe(df, <table-name>)

    but can also be used without writing a single line of code.
    Nothing is returned.
    """

    class_name = "CreateTable"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        create_table = rel.create_table()

        dask_table = rel.getTable()
        schema_name, table_name = [n.lower() for n in context.fqn(dask_table)]

        if table_name in context.schema[schema_name].tables:
            if create_table.getIfNotExists():
                return
            elif not create_table.getOrReplace():
                raise RuntimeError(
                    f"A table with the name {table_name} is already present."
                )

        kwargs = convert_sql_kwargs(create_table.getSQLWithOptions())

        logger.debug(
            f"Creating new table with name {table_name} and parameters {kwargs}"
        )

        format = kwargs.pop("format", None)
        if format:  # pragma: no cover
            format = format.lower()
        persist = kwargs.pop("persist", False)

        try:
            location = kwargs.pop("location")
        except KeyError:
            raise AttributeError("Parameters must include a 'location' parameter.")

        gpu = kwargs.pop("gpu", False)
        context.create_table(
            table_name,
            location,
            format=format,
            persist=persist,
            schema_name=schema_name,
            gpu=gpu,
            **kwargs,
        )
