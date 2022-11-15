import logging
from typing import TYPE_CHECKING

import numpy as np
from dask import delayed

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.utils import convert_sql_kwargs, import_class

if TYPE_CHECKING:
    import dask_sql
    from dask_sql.rust import LogicalPlan

logger = logging.getLogger(__name__)


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
      You might need to install necessary packages to use
      the models.
    * target_column: Which column from the data to use as target.
      If not empty, it is removed automatically from
      the training data. Defaults to an empty string, in which
      case no target is feed to the model training (e.g. for
      unsupervised algorithms). This means, you typically
      want to set this parameter.
    * wrap_predict: Boolean flag, whether to wrap the selected
      model with a :class:`dask_sql.physical.rel.custom.wrappers.ParallelPostFit`.
      Defaults to false. Typically you set it to true for
      sklearn models if predicting on big data.
    * wrap_fit: Boolean flag, whether to wrap the selected
      model with a :class:`dask_sql.physical.rel.custom.wrappers.Incremental`.
      Defaults to false. Typically you set it to true for
      sklearn models if training on big data.
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
            model_class = 'xgboost.XGBClassifier',
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
          of data but predicting on large data samples,
          you might want to set `wrap_predict` to True.
          With this option, model interference will be
          parallelized/distributed.
        * If you are training on large amounts of data,
          you can try setting wrap_fit to True. This will
          do the same on the training step, but works only on
          those models, which have a `fit_partial` method.
    """

    class_name = "CreateModel"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        create_model = rel.create_model()
        select = create_model.getSelectQuery()

        schema_name, model_name = context.schema_name, create_model.getModelName()
        kwargs = convert_sql_kwargs(create_model.getSQLWithOptions())

        if model_name in context.schema[schema_name].models:
            if create_model.getIfNotExists():
                return
            elif not create_model.getOrReplace():
                raise RuntimeError(
                    f"A model with the name {model_name} is already present."
                )

        logger.debug(
            f"Creating model {model_name} from query {select} with options {kwargs}"
        )

        try:
            model_class = kwargs.pop("model_class")
        except KeyError:
            raise ValueError("Parameters must include a 'model_class' parameter.")

        target_column = kwargs.pop("target_column", "")
        wrap_predict = kwargs.pop("wrap_predict", False)
        wrap_fit = kwargs.pop("wrap_fit", False)
        fit_kwargs = kwargs.pop("fit_kwargs", {})

        training_df = context.sql(select)

        if target_column:
            non_target_columns = [
                col for col in training_df.columns if col != target_column
            ]
            X = training_df[non_target_columns]
            y = training_df[target_column]
        else:
            X = training_df
            y = None

        try:
            ModelClass = import_class(model_class)
        except ImportError:
            raise ValueError(
                f"Can not import model {model_class}. Make sure you spelled it correctly and have installed all packages."
            )

        model = ModelClass(**kwargs)
        if wrap_fit:
            from dask_sql.physical.rel.custom.wrappers import Incremental

            model = Incremental(estimator=model)

        if wrap_predict:
            from dask_sql.physical.rel.custom.wrappers import ParallelPostFit

            # When `wrap_predict` is set to True we train on single partition frames
            # because this is only useful for non dask distributed models
            # Training via delayed fit ensures that we dont have to transfer
            # data back to the client for training

            X_d = X.repartition(npartitions=1).to_delayed()
            if y is not None:
                y_d = y.repartition(npartitions=1).to_delayed()
            else:
                y_d = None

            delayed_model = [delayed(model.fit)(x_p, y_p) for x_p, y_p in zip(X_d, y_d)]
            model = delayed_model[0].compute()
            if "sklearn" in model_class:
                output_meta = np.array([])
                model = ParallelPostFit(
                    estimator=model,
                    predict_meta=output_meta,
                    predict_proba_meta=output_meta,
                    transform_meta=output_meta,
                )
            else:
                model = ParallelPostFit(estimator=model)

        else:
            model.fit(X, y, **fit_kwargs)
        context.register_model(model_name, model, X.columns, schema_name=schema_name)
