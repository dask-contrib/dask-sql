import logging
import copy
import uuid

from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.java import org, java

logger = logging.getLogger(__name__)


class PredictModelPlugin(BaseRelPlugin):
    """
    TODO
    """

    class_name = "com.dask.sql.parser.SqlPredictModel"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        sql_select = sql.getSelect()
        model_name = str(sql.getModelName())
        select_list = sql.getSelectList()

        logger.debug(
            f"Predicting from {model_name} and query {sql_select} to {list(select_list)}"
        )

        sql_select_query = context._to_sql_string(sql_select)
        df = context.sql(sql_select_query)

        # TODO: do magic with prediction and storing in temporary table
        predicted_df = df.copy()

        # Create a temporary context, which includes the
        # new "table"
        temporary_table = str(uuid.uuid4())
        tmp_context = copy.deepcopy(context)
        tmp_context.create_table(temporary_table, predicted_df)

        sql_ns = org.apache.calcite.sql
        pos = sql.getParserPosition()
        from_clause = sql_ns.SqlIdentifier(
            java.util.List.of(temporary_table), pos
        )  # TODO: correct pos

        outer_select = sql_ns.SqlSelect(
            sql.getParserPosition(),
            None,  # keywordList,
            select_list,  # selectList,
            from_clause,  # from,
            None,  # where,
            None,  # groupBy,
            None,  # having,
            None,  # windowDecls,
            None,  # orderBy,
            None,  # offset,
            None,  # fetch,
            None,  # hints
        )

        sql_outer_query = tmp_context._to_sql_string(outer_select)
        df = tmp_context.sql(sql_outer_query)

        cc = ColumnContainer(df.columns)
        dc = DataContainer(df, cc)

        return dc
