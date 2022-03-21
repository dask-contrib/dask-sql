from typing import TYPE_CHECKING
import logging

from dask_planner.rust import (
    DaskFunction,
    DaskSchema,
    DaskTable,
    Query,
    Statement,
    LogicalPlan,
    LogicalPlanGenerator,
)

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql

logger = logging.getLogger(__name__)

class DaskTableScanPlugin(BaseRelPlugin):
    """
    A DaskTableScal is the main ingredient: it will get the data
    from the database. It is always used, when the SQL looks like

        SELECT .... FROM table ....

    We need to get the dask dataframe from the registered
    tables and return the requested columns from it.
    Calcite will always refer to columns via index.
    """

    class_name = "TableScan"

    def convert(
        self,
        input_dc: DataContainer,
        logical_generator: LogicalPlanGenerator,
        rel: LogicalPlan,
        context: "dask_sql.Context"
    ) -> DataContainer:
        field_names = rel.get_field_names()

        # The table(s) we need to return
        table = rel.table()

        # The table names are all names split by "."
        # We assume to always have the form something.something
        table_names = [str(n) for n in table.get_qualified_name()]
        assert len(table_names) == 2
        schema_name = table_names[0]
        table_name = table_names[1]
        table_name = table_name.lower()

        logger.debug(f"table_scan.convert() -> schema_name: {schema_name} - table_name: {table_name}")

        dc = context.schema[schema_name].tables[table_name]
        df = dc.df
        cc = dc.column_container

        # Make sure we only return the requested columns
        cc = cc.limit_to(field_names)

        cc = self.fix_column_to_row_type(cc, table.column_names())
        dc = DataContainer(df, cc)
        dc = self.fix_dtype_to_row_type(dc, table)
        return dc
