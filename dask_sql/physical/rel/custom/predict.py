import logging
import uuid
from typing import TYPE_CHECKING

import numpy as np

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

    class_name = "PredictModel"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        predict_model = rel.predict_model()

        sql_select = predict_model.getSelect()

        # The table(s) we need to return
        dask_table = rel.getTable()
        schema_name, model_name = [n.lower() for n in context.fqn(dask_table)]

        model, training_columns = context.schema[schema_name].models[model_name]
        df = context.sql(sql_select)
        part = df[training_columns]
        try:
            output_meta = model.predict_meta
        except AttributeError:
            output_meta = None
        if part.shape[0].compute() == 0 and output_meta is not None:
            empty_output = self.handle_empty_partitions(output_meta)
            if empty_output is not None:
                return empty_output
        prediction = model.predict(part)
        predicted_df = df.assign(target=prediction)

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

    def handle_empty_partitions(self, output_meta):
        if hasattr(output_meta, "__array_function__"):
            if len(output_meta.shape) == 1:
                shape = 0
            else:
                shape = list(output_meta.shape)
                shape[0] = 0
            ar = np.zeros(
                shape=shape,
                dtype=output_meta.dtype,
                like=output_meta,
            )
            return ar
        elif "scipy.sparse" in type(output_meta).__module__:
            # sparse matrices don't support
            # `like` due to non implimented __array_function__
            # Refer https://github.com/scipy/scipy/issues/10362
            # Note below works for both cupy and scipy sparse matrices
            if len(output_meta.shape) == 1:
                shape = 0
            else:
                shape = list(output_meta.shape)
                shape[0] = 0

            ar = type(output_meta)(shape, dtype=output_meta.dtype)
            return ar
        elif hasattr(output_meta, "iloc"):
            return output_meta.iloc[:0, :]
