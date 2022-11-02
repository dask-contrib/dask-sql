import logging
from typing import TYPE_CHECKING

import dask.dataframe as dd
import pandas as pd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.utils import convert_sql_kwargs, import_class

if TYPE_CHECKING:
    import dask_sql
    from dask_sql.rust import LogicalPlan

logger = logging.getLogger(__name__)


class CreateExperimentPlugin(BaseRelPlugin):
    """
    Creates an  Experiment for hyperparameter tuning or automl like behaviour,
    i.e evaluates models with different hyperparameters and registers the best performing
    model in the context with the name same as experiment name,
    which can be used for prediction

    sql syntax:
        CREATE EXPERIMENT <name> WITH ( key = value )
            AS <some select query>

    OPTIONS:
    * model_class: Full path to the class of the model which has to be tuned.
      Any model class with sklearn interface is valid, but might or
      might not work well with Dask dataframes.
      You might need to install necessary packages to use
      the models.
    * experiment_class : Full path of the Hyperparameter tuner
    * tune_parameters:
      Key-value of pairs of Hyperparameters to tune, i.e Search Space for
      particular model to tune
    * automl_class : Full path of the class which is sklearn compatible and
      able to distribute work to dask clusters, currently tested with
      tpot automl framework.
      Refer : [Tpot example](https://examples.dask.org/machine-learning/tpot.html)
    * target_column: Which column from the data to use as target.
      Currently this parameter is required field, because tuning and automl
      behaviour is implemented only for supervised algorithms.
    * automl_kwargs:
      Key-value pairs of arguments to be passed to automl class .
      Refer : [Using Tpot parameters](https://epistasislab.github.io/tpot/using/)
    * experiment_kwargs:
      Use this parameter for passing any keyword arguments to experiment class
    * tune_fit_kwargs:
      Use this parameter for passing any keyword arguments to experiment.fit() method

      example:
        for Hyperparameter tuning  : (Train and evaluate same model with different parameters)

            CREATE EXPERIMENT my_exp WITH(
            model_class = 'sklearn.ensemble.GradientBoostingClassifier',
            experiment_class = 'sklearn.model_selection.GridSearchCV',
            tune_parameters = (n_estimators = ARRAY [16, 32, 2],
                                learning_rate = ARRAY [0.1,0.01,0.001],
                               max_depth = ARRAY [3,4,5,10]
                               ),
            target_column = 'target'
            ) AS (
                    SELECT x, y, x*y > 0 AS target
                    FROM timeseries
                    LIMIT 100
                )

       for automl : (Train different different model with different parameter)

            CREATE EXPERIMENT my_exp WITH (
            automl_class = 'tpot.TPOTClassifier',
            automl_kwargs = (population_size = 2 ,
            generations=2,
            cv=2,
            n_jobs=-1,
            use_dask=True,
            max_eval_time_mins=1),
            target_column = 'target'
            ) AS (
                SELECT x, y, x*y > 0 AS target
                FROM timeseries
                LIMIT 100
            )

    """

    class_name = "CreateExperiment"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        create_experiment = rel.create_experiment()
        select = create_experiment.getSelectQuery()

        schema_name, experiment_name = (
            context.schema_name,
            create_experiment.getExperimentName(),
        )
        kwargs = convert_sql_kwargs(create_experiment.getSQLWithOptions())

        if experiment_name in context.schema[schema_name].experiments:
            if create_experiment.getIfNotExists():
                return
            elif not create_experiment.getOrReplace():
                raise RuntimeError(
                    f"A experiment with the name {experiment_name} is already present."
                )

        logger.debug(
            f"Creating Experiment {experiment_name} from query {select} with options {kwargs}"
        )
        model_class = None
        automl_class = None
        experiment_class = None
        if "model_class" in kwargs:
            model_class = kwargs.pop("model_class")
            # when model class was provided, must provide experiment_class also for tuning
            if "experiment_class" not in kwargs:
                raise ValueError(
                    f"Parameters must include a 'experiment_class' parameter for tuning {model_class}."
                )
            experiment_class = kwargs.pop("experiment_class")
        elif "automl_class" in kwargs:
            automl_class = kwargs.pop("automl_class")
        else:
            raise ValueError(
                "Parameters must include a 'model_class' or 'automl_class' parameter."
            )
        target_column = kwargs.pop("target_column", "")
        tune_fit_kwargs = kwargs.pop("tune_fit_kwargs", {})
        parameters = kwargs.pop("tune_parameters", {})
        experiment_kwargs = kwargs.pop("experiment_kwargs", {})
        automl_kwargs = kwargs.pop("automl_kwargs", {})
        logger.info(parameters)

        training_df = context.sql(select)
        if not target_column:
            raise ValueError(
                "Unsupervised Algorithm cannot be tuned Automatically,"
                "Consider providing 'target column'"
            )
        non_target_columns = [
            col for col in training_df.columns if col != target_column
        ]
        X = training_df[non_target_columns]
        y = training_df[target_column]

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
                    f"Can not import tuner {experiment_class}. Make sure you spelled it correctly and have installed all packages."
                )

            from dask_sql.physical.rel.custom.wrappers import ParallelPostFit

            model = ModelClass()

            search = ExperimentClass(model, {**parameters}, **experiment_kwargs)
            logger.info(tune_fit_kwargs)
            search.fit(
                X.to_dask_array(lengths=True),
                y.to_dask_array(lengths=True),
                **tune_fit_kwargs,
            )
            df = pd.DataFrame(search.cv_results_)
            df["model_class"] = model_class

            context.register_model(
                experiment_name,
                ParallelPostFit(estimator=search.best_estimator_),
                X.columns,
                schema_name=schema_name,
            )

        if automl_class:

            try:
                AutoMLClass = import_class(automl_class)
            except ImportError:
                raise ValueError(
                    f"Can not import automl model {automl_class}. Make sure you spelled it correctly and have installed all packages."
                )

            from dask_sql.physical.rel.custom.wrappers import ParallelPostFit

            automl = AutoMLClass(**automl_kwargs)
            # should be avoided if  data doesn't fit in memory
            automl.fit(X.compute(), y.compute())
            df = (
                pd.DataFrame(automl.evaluated_individuals_)
                .T.reset_index()
                .rename({"index": "models"}, axis=1)
            )

            context.register_model(
                experiment_name,
                ParallelPostFit(estimator=automl.fitted_pipeline_),
                X.columns,
                schema_name=schema_name,
            )

        context.register_experiment(
            experiment_name, experiment_results=df, schema_name=schema_name
        )
        cc = ColumnContainer(df.columns)
        dc = DataContainer(dd.from_pandas(df, npartitions=1), cc)
        return dc
