import logging
import pandas as pd
import dask.dataframe as dd

from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.datacontainer import DataContainer
from dask_sql.mappings import sql_to_python_value

logger = logging.getLogger(__name__)


def convert_literal(value):
    literal_type = str(value.getTypeName())

    if literal_type == "CHAR":
        return str(value.getStringValue())

    literal_value = value.getValue()
    python_value = sql_to_python_value(literal_type, literal_value)
    return python_value


class CreateTablePlugin(BaseRelPlugin):
    """
    Create a table with given parameters
    """

    class_name = "com.dask.sql.parser.SqlCreateTable"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        kwargs = {
            str(key): convert_literal(value)
            for key, value in dict(sql.getKwargs()).items()
        }

        table_name = str(sql.getTableName())

        logger.debug(
            f"Creating new table with name {table_name} and parameters {kwargs}"
        )

        format = kwargs.pop("format", "csv").lower()
        persist = kwargs.pop("persist", False)

        try:
            location = kwargs.pop("location")
        except KeyError:
            raise AttributeError("Parameters must include a 'location' parameter.")

        read_function_name = f"read_{format}"

        try:
            read_function = getattr(dd, read_function_name)
        except AttributeError:
            raise AttributeError(f"Do not understand input format {format}.")

        df = read_function(location, **kwargs)

        if persist:
            df = df.persist()

        context.register_dask_table(df, table_name)
