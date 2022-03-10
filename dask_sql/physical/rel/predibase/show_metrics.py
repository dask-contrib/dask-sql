from typing import TYPE_CHECKING

import logging
import dask.dataframe as dd
import pandas as pd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_sql.java import org

logger = logging.getLogger(__name__)

class ShowMetricsPlugin(BaseRelPlugin):
    """
    Show METRICS for the current model
    The SQL is:

        SHOW METRICS [<metric>, [<metric> ...]] FROM <model>

    The result is also a table, although it is created on the fly.
    """

    class_name = "com.predibase.pql.parser.SqlShowMetrics"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        # Build up the select list of metrics
        if sql.getMetricList():
            metric_list = [context.fqn(col)[1] for col in sql.getMetricList()]
            metric_cols = "model_name,target_name," + ",".join(metric_list)
        else:
            metric_cols = "*"
        outer_select = f"select {metric_cols} from model_metrics"
        # Get filter for target list of model
        if sql.getTargetList():
            field_name = "target_name"
            field_list = [context.fqn(col)[1] for col in sql.getTargetList()]
        elif sql.getModelList():
            field_name = "model_name"
            field_list = [context.fqn(col.getName())[1] for col in sql.getModelList()]
        if field_list:
            field_cols = ",".join([f"'{f}'" for f in field_list])
            outer_select += f" where {field_name} in ({field_cols})"
        # Build up the metrics select statement
        logger.debug(
            f"Metrics query: {outer_select}"
        )

        sql_outer_query = context._to_sql_string(outer_select)
        df = context.sql(sql_outer_query)

        cc = ColumnContainer(df.columns)
        dc = DataContainer(df, cc)

        return dc
