import logging
import copy
import uuid

from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.java import org, com, java

logger = logging.getLogger(__name__)


class PredictModelPlugin(BaseRelPlugin):
    """
    Predict the target using the given model and dataframe from the SELECT query.

    The SQL call looks like

        SELECT <cols> FROM PREDICT (MODEL <model-name>, <some select query>)

    The return value is the input dataframe with an additional column named
    "target", which contains the predicted values.
    The model needs to be registered at the context before using it in this function,
    either by calling :ref:`register_model` explicitly or by training
    a model using the `CREATE MODEL` SQL statement.

    A model can be anything which has a `predict` function.
    Please note however, that it will need to act on Dask dataframes. If you
    are using a model not optimized for this, it might be that you run out of memory if
    your data is larger than the RAM of a single machine.
    To prevent this, have a look into the dask-ml package,
    especially the [ParallelPostFit](https://ml.dask.org/meta-estimators.html)
    meta-estimator. If you are using a model trained with `CREATE MODEL`
    and the `wrap_predict` flag, this is done automatically.

    Using this SQL is roughly equivalent to doing

        df = context.sql("<select query>")
        model = get the model from the context

        target = model.predict(df)
        return df.assign(target=target)

    but can also be used without writing a single line of code.
    """

    class_name = "com.dask.sql.parser.SqlPredictModel"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        sql_select = sql.getSelect()
        model_name = str(sql.getModelName().getIdentifier())
        model_type = sql.getModelName().getIdentifierType()
        select_list = sql.getSelectList()

        logger.debug(
            f"Predicting from {model_name} and query {sql_select} to {list(select_list)}"
        )

        IdentifierType = com.dask.sql.parser.SqlModelIdentifier.IdentifierType

        if model_type == IdentifierType.REFERENCE:
            try:
                model, training_columns = context.models[model_name]
            except KeyError:
                raise KeyError(f"No model registered with name {model_name}")
        else:
            raise NotImplementedError(f"Do not understand model type {model_type}")

        sql_select_query = context._to_sql_string(sql_select)
        df = context.sql(sql_select_query)

        prediction = model.predict(df[training_columns])
        predicted_df = df.assign(target=prediction)

        # Create a temporary context, which includes the
        # new "table" so that we can use the normal
        # SQL-to-dask-code machinery
        while True:
            # Make sure to choose a non-used name
            temporary_table = str(uuid.uuid4())
            if temporary_table not in context.tables:
                break
            else:  # pragma: no cover
                continue

        tmp_context = copy.deepcopy(context)
        tmp_context.create_table(temporary_table, predicted_df)

        sql_ns = org.apache.calcite.sql
        pos = sql.getParserPosition()
        from_column_list = java.util.ArrayList()
        from_column_list.add(temporary_table)
        from_clause = sql_ns.SqlIdentifier(from_column_list, pos)  # TODO: correct pos

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
