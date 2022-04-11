import logging
from typing import TYPE_CHECKING

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.utils import LoggableDataFrame

if TYPE_CHECKING:
    import dask_sql
    from dask_sql.java import org

logger = logging.getLogger(__name__)


class DistributeByPlugin(BaseRelPlugin):
    """
    Distribute the target based on the specified sql identifier from a SELECT query.
    The SQL is:

        SELECT age, name FROM person DISTRIBUTE BY age
    """

    class_name = "com.dask.sql.parser.SqlDistributeBy"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        select = sql.getSelect()
        distribute_list = [str(col) for col in sql.getDistributeList()]

        sql_select_query = context._to_sql_string(select)
        df = context.sql(sql_select_query)
        logger.debug(f"Extracted sub-dataframe as {LoggableDataFrame(df)}")

        logger.debug(f"Will now shuffle according to {distribute_list}")

        # Perform the distribute by operation via a Dask shuffle
        df = df.shuffle(distribute_list)

        cc = ColumnContainer(df.columns)
        dc = DataContainer(df, cc)

        return dc
