import logging
import uuid
from typing import TYPE_CHECKING

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.java import com, java, org
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql

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

    class_name = "com.predibase.pql.parser.SqlPredict"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        # TODO: How to handle multiple selects / given items
        sql_select = sql.getGivenSelect()[0]

        # TODO: Handle multiple targets here
        target_list = sql.getTargetList()
        _, target_name = context.fqn(target_list[0])

        # Get model name
        if sql.getModel():
            schema_name, model_name = context.fqn(sql.getModel().getName())
        else:
            # Use default context to get first model_name from target_name
            schema_name = context.schema_name
            model_name = context.schema[context.schema_name].targets[target_name][0]

        # Get the select list that includes target names, and select column
        select_list = sql.getSelectList()

        logger.debug(
            f"Predicting from {model_name} and query {sql_select} targets {target_name}"
        )

        model, training_columns = context.schema[schema_name].models[model_name]        
        sql_select_query = context._to_sql_string(sql_select)
        df = context.sql(sql_select_query)

        # Make prediction and save against target in dataframe
        prediction = model.predict(df[training_columns])
        df[target_name] = prediction

        is_temporary = not sql.getInto()
        if is_temporary:
            # Create a temporary context, which includes the
            # new "table" so that we can use the normal
            # SQL-to-dask-code machinery
            while True:
                # Make sure to choose a non-used name
                target_table = str(uuid.uuid4())
                if target_table not in context.schema[schema_name].tables:
                    break
                else:  # pragma: no cover
                    continue
        else:
            # Get into table name
            _, target_table = context.fqn(sql.getInto().getName())

        context.create_table(target_table, df)

        sql_ns = org.apache.calcite.sql
        pos = sql.getParserPosition()
        from_column_list = java.util.ArrayList()
        from_column_list.add(target_table)

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

        sql_outer_query = context._to_sql_string(outer_select)
        df = context.sql(sql_outer_query)

        # If temporary drop target table
        if is_temporary:
            context.drop_table(target_table)

        cc = ColumnContainer(df.columns)
        dc = DataContainer(df, cc)

        return dc
