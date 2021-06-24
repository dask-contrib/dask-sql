import logging

import dask.dataframe as dd
import pandas as pd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.java import org
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.utils import convert_sql_kwargs, import_class

logger = logging.getLogger(__name__)


class CreateExperimentPlugin(BaseRelPlugin):
    """
    Create Experiment for hyperparameter tunning or Automl like behaviour

    """

    class_name = "com.dask.sql.parser.SqlCreateExperiment"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        select = sql.getSelect()
        experiment_name = str(sql.getExperimentName())
        kwargs = convert_sql_kwargs(sql.getKwargs())

        if experiment_name in context.experiments:
            if sql.getIfNotExists():
                return
            elif not sql.getReplace():
                raise RuntimeError(
                    f"A experiment with the name {experiment_name} is already present."
                )

        logger.debug(
            f"Creating Experiment {experiment_name} from query {select} with options {kwargs}"
        )
        model_class = None
        automl_class = None
        experiment_class = None
        if "model_class" not in kwargs and "automl_class" not in kwargs:
            # if it is not automl , model_class must be provided
            raise ValueError(
                "Parameters must include a 'model_class' or 'automl_class' parameter."
            )
        elif "model_class" in kwargs:
            model_class = kwargs.pop("model_class")
            # when model class was provided, must provide hyperparameter class for tuning
            if "experiment_class" not in kwargs:
                raise ValueError(
                    f"Parameters must include a 'experiment_class' parameter for tunning {model_class}."
                )
            experiment_class = kwargs.pop("experiment_class")
        elif "automl_class" in kwargs:
            automl_class = kwargs.pop("automl_class")

        target_column = kwargs.pop("target_column", "")
        wrap_fit = kwargs.pop("wrap_fit", False)
        tune_fit_kwargs = kwargs.pop("tune_fit_kwargs", {})
        parameters = kwargs.pop("tune_parameters", {})
        experiment_kwargs = kwargs.pop("experiment_kwargs", {})
        automl_kwargs = kwargs.pop("automl_kwargs", {})
        logger.info(parameters)
        # tune_kwargs = eval(tune_kwargs)
        # print(tune_kwargs)
        # print(eval(tune_kwargs))
        # tune_kwargs = {}
        # for k,v in _tune_kwargs:
        #     tune_kwargs[k] = eval(str(v))
        # According to dask docs,
        # https://ml.dask.org/incremental.html wrap_fit doesnt go well hyperparameter optimization

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

        if model_class and experiment_class:
            try:
                ModelClass = import_class(model_class)
            except ImportError:
                raise ValueError(
                    f"Can not import model {model_class}. Make sure you spelled it correctly and have installed all packages."
                )
            try:
                ExperimentClass = import_class(experiment_class)
            except ImportError:
                raise ValueError(
                    f"Can not import model {experiment_class}. Make sure you spelled it correctly and have installed all packages."
                )

            model = ModelClass()

            search = ExperimentClass(model, {**parameters}, **experiment_kwargs)
            logger.info(tune_fit_kwargs)
            search.fit(X, y, **tune_fit_kwargs)
            model_name = (
                experiment_name + "_" + model_class.rsplit(".")[-1] + "_best_model"
            )
            df = pd.DataFrame(search.cv_results_)
            df["model_class"] = model_class
            from dask_ml.wrappers import ParallelPostFit

            context.register_model(
                model_name, ParallelPostFit(estimator=search.best_estimator_), X.columns
            )

        elif automl_class:
            try:
                AutoMLClass = import_class(automl_class)
            except ImportError:
                raise ValueError(
                    f"Can not import model {automl_class}. Make sure you spelled it correctly and have installed all packages."
                )
            # implement TPOTClassifier
            automl = AutoMLClass(**automl_kwargs)
            # should be avoided if  data doesn't fit in memory
            automl.fit(X.compute(), y.compute())
            df = (
                pd.DataFrame(automl.evaluated_individuals_)
                .T.reset_index()
                .rename({"index": "models"}, axis=1)
            )
            model_name = "automl_" + automl_class.rsplit(".")[-1]
            from dask_ml.wrappers import ParallelPostFit

            context.register_model(
                model_name,
                ParallelPostFit(estimator=automl.fitted_pipeline_),
                X.columns,
            )

        context.register_experiment(experiment_name, experiment_results=df)
        cc = ColumnContainer(df.columns)
        dc = DataContainer(dd.from_pandas(df, npartitions=1), cc)
        return dc
