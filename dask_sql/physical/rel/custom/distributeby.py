import copy
import logging
import uuid

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.java import com, java, org
from dask_sql.physical.rel.base import BaseRelPlugin

logger = logging.getLogger(__name__)


class DistributeByPlugin(BaseRelPlugin):
    """
    Distribute the target based on the specified sql identifier from a SELECT query.
    The SQL is:

        SELECT age, name FROM person DISTRIBUTE BY age

    HERE, What do we actually return here???
    """

    class_name = "com.dask.sql.parser.SqlDistributeBy"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:

        select_list = sql.getSelectList()
        distribute_list = sql.getDistributeList()

        # Hardcoded testing here, need to make changes to Java grammar parser to remove this.
        select_query = context._to_sql_string(
            "SELECT tips.size, tips.total_bill FROM tips"
        )
        df = context.sql(select_query)

        # Perform the distribute by operation via a Dask shuffle
        df = df.shuffle("size")

        cc = ColumnContainer(df.columns)
        dc = DataContainer(df, cc)

        return dc
