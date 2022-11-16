import logging
import uuid
from typing import TYPE_CHECKING

import dask.dataframe as dd
import pandas as pd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan

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
    To prevent this, have a look into the dask_sql.physical.rel.custom.wrappers.ParallelPostFit
    meta-estimator. If you are using a model trained with `CREATE MODEL`
    and the `wrap_predict` flag, this is done automatically.

    Using this SQL is roughly equivalent to doing

        df = context.sql("<select query>")
        model = get the model from the context

        target = model.predict(df)
        return df.assign(target=target)

    but can also be used without writing a single line of code.
    """

    class_name = "PredictModel"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        predict_model = rel.predict_model()

        sql_select = predict_model.getSelect()

        # The table(s) we need to return
        dask_table = rel.getTable()
        schema_name, model_name = [n.lower() for n in context.fqn(dask_table)]

        model, training_columns = context.schema[schema_name].models[model_name]
        df = context.sql(sql_select)
        try:
            prediction = model.predict(df[training_columns])
            predicted_df = df.assign(target=prediction)
        except TypeError:
            df = df.set_index(df.columns[0], drop=False)
            prediction = model.predict(df[training_columns])
            # Convert numpy.ndarray to Dask Series
            prediction = dd.from_pandas(
                pd.Series(prediction, index=df.index),
                npartitions=df.npartitions,
            )
            predicted_df = df.assign(target=prediction)
            # Need to drop first column to reset index
            # because the first column is equal to the index
            predicted_df = predicted_df.drop(columns=[df.columns[0]]).reset_index()

        # Create a temporary context, which includes the
        # new "table" so that we can use the normal
        # SQL-to-dask-code machinery
        while True:
            # Make sure to choose a non-used name
            temporary_table = str(uuid.uuid4())
            if temporary_table not in context.schema[schema_name].tables:
                break
            else:  # pragma: no cover
                continue

        context.create_table(temporary_table, predicted_df)

        cc = ColumnContainer(predicted_df.columns)
        dc = DataContainer(predicted_df, cc)

        return dc
