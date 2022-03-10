import logging
from typing import TYPE_CHECKING

from dask import delayed
import dask.array as da
import dask.dataframe as dd
import numpy as np


from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.utils import convert_sql_kwargs, import_class

if TYPE_CHECKING:
    import dask_sql
    from dask_sql.java import org

logger = logging.getLogger(__name__)

class DummyModel(object):
    """
    Dummy model that returns x*y predictions
    """
    def __init__(self, model_name):
        self.model_name = model_name

    def predict(self, df: dd.DataFrame) -> dd.DataFrame:
        return df['x'] * df['y']


class CreateModelPlugin(BaseRelPlugin):
    """
    Create and train a model on the data from the given SELECT query
    and register it at the context.
    The SQL call looks like

        CREATE MODEL <model-name> WITH ( key = value )
            AS <some select query>

    It sends the select query through the normal parsing
    and optimization and conversation and uses the result
    as the training input.

    The options control, how and which model is trained:
    * model_class: Full path to the class of the model to train.
      Any model class with sklearn interface is valid, but might or
      might not work well with Dask dataframes.
      Have a look into the
      [dask-ml documentation](https://ml.dask.org/index.html)
      for more information on which models work best.
      You might need to install necessary packages to use
      the models.
    * target_column: Which column from the data to use as target.
      If not empty, it is removed automatically from
      the training data. Defaults to an empty string, in which
      case no target is feed to the model training (e.g. for
      unsupervised algorithms). This means, you typically
      want to set this parameter.
    * wrap_predict: Boolean flag, whether to wrap the selected
      model with a :class:`dask_ml.wrappers.ParallelPostFit`.
      Have a look into the
      [dask-ml docu](https://ml.dask.org/meta-estimators.html#parallel-prediction-and-transformation)
      to learn more about it. Defaults to false. Typically you set
      it to true for sklearn models if predicting on big data.
    * wrap_fit: Boolean flag, whether to wrap the selected
      model with a :class:`dask_ml.wrappers.Incremental`.
      Have a look into the
      [dask-ml docu](https://ml.dask.org/incremental.html)
      to learn more about it. Defaults to false. Typically you set
      it to true for sklearn models if training on big data.
    * fit_kwargs: keyword arguments sent to the call to fit().

    All other arguments are passed to the constructor of the
    model class.

    Using this SQL is roughly equivalent to doing

        df = context.sql("<select query>")
        X = df[everything except target_column]
        y = df[target_column]
        model = ModelClass(**kwargs)

        model = model.fit(X, y, **fit_kwargs)
        context.register_model(<model-name>, model)

    but can also be used without writing a single line of code.
    Nothing is returned.

    Examples:

        CREATE MODEL my_model WITH (
            model_class = 'dask_ml.xgboost.XGBClassifier',
            target_column = 'target'
        ) AS (
            SELECT x, y, target
            FROM "data"
        )

    Notes:

        This SQL call is not a 1:1 replacement for a normal
        python training and can not fulfill all use-cases
        or requirements!

        If you are dealing with large amounts of data,
        you might run into problems while model training and/or
        prediction, depending if your model can cope with
        dask dataframes.

        * if you are training on relatively small amounts
          of data but predicting on large data samples
          (and you are not using a model build for usage with dask
          from the dask-ml package), you might want to set
          `wrap_predict` to True. With this option,
          model interference will be parallelized/distributed.
        * If you are training on large amounts of data,
          you can try setting wrap_fit to True. This will
          do the same on the training step, but works only on
          those models, which have a `fit_partial` method.
    """

    class_name = "com.predibase.pql.parser.SqlCreateModel"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        select = sql.getQuery()
        schema_name, model_name = context.fqn(sql.getName())
        if model_name in context.schema[schema_name].models:
            if sql.ifNotExists:
                return
            elif not sql.getReplace():
                raise RuntimeError(
                    f"A model with the name {model_name} is already present."
                )

        # TODO: handle multiple target columns
        targets = sql.getTargetList()
        _, target_column = context.fqn(targets[0])

        logger.debug(
            f"Creating model {model_name} for {target_column} from query {select}"
        )

        # Assume that create model has select
        select_query = context._to_sql_string(select)
        training_df = context.sql(select_query)

        if target_column:
            non_target_columns = [
                col for col in training_df.columns if col != target_column
            ]
            X = training_df[non_target_columns]
            y = training_df[target_column]
        else:
            X = training_df
            y = None

        # TODO: Create a class that has a prediction method
        model = DummyModel(model_name)

        context.register_model(model_name, model,
            training_columns=X.columns,
            target_column=target_column,
            schema_name=schema_name)
