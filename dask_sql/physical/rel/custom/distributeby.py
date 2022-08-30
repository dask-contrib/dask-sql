import logging
from typing import TYPE_CHECKING

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.utils import LoggableDataFrame

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan

logger = logging.getLogger(__name__)


class DistributeByPlugin(BaseRelPlugin):
    """
    Distribute the target based on the specified sql identifier from a SELECT query.
    The SQL is:

        SELECT age, name FROM person DISTRIBUTE BY age
    """

    # DataFusion provides the phrase `Repartition` in the LogicalPlan instead of `Distribute By`, it is the same thing
    class_name = "Repartition"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        distribute = rel.repartition_by()
        select = distribute.getSelectQuery()
        distribute_list = distribute.getDistributionColumns()

        df = context.sql(select)
        logger.debug(f"Extracted sub-dataframe as {LoggableDataFrame(df)}")

        logger.debug(f"Will now shuffle according to {distribute_list}")

        # Perform the distribute by operation via a Dask shuffle
        df = df.shuffle(distribute_list)

        cc = ColumnContainer(df.columns)
        dc = DataContainer(df, cc)

        return dc
