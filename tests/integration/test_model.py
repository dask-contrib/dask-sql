import os
import pickle

import joblib
import pandas as pd
import pytest
from dask.datasets import timeseries
from mock import MagicMock


def check_trained_model(c, model_name=None):
    if model_name is None:
        sql = """
        SELECT * FROM PREDICT(
            MODEL my_model,
            SELECT x, y FROM timeseries
        )
        """
    else:
        sql = f"""
        SELECT * FROM PREDICT(
            MODEL {model_name},
            SELECT x, y FROM timeseries
        )
        """

    result_df = c.sql(sql).compute()

    assert "target" in result_df.columns
    assert len(result_df["target"]) > 0


@pytest.fixture()
def training_df(c):
    df = timeseries(freq="1d").reset_index(drop=True)
    c.create_table("timeseries", df, persist=True)

    return training_df


def test_training_and_prediction(c, training_df):
    c.sql(
        """
        CREATE MODEL my_model WITH (
            model_class = 'sklearn.ensemble.GradientBoostingClassifier',
            wrap_predict = True,
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
    """
    )

    check_trained_model(c)


def test_clustering_and_prediction(c, training_df):
    c.sql(
        """
        CREATE MODEL my_model WITH (
            model_class = 'dask_ml.cluster.KMeans'
        ) AS (
            SELECT x, y
            FROM timeseries
            LIMIT 100
        )
    """
    )

    check_trained_model(c)


def test_iterative_and_prediction(c, training_df):
    c.sql(
        """
        CREATE MODEL my_model WITH (
            model_class = 'sklearn.linear_model.SGDClassifier',
            wrap_fit = True,
            target_column = 'target',
            fit_kwargs = ( classes = ARRAY [0, 1] )
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
    """
    )

    check_trained_model(c)


def test_show_models(c, training_df):
    c.sql(
        """
        CREATE MODEL my_model1 WITH (
            model_class = 'sklearn.ensemble.GradientBoostingClassifier',
            wrap_predict = True,
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
    """
    )
    c.sql(
        """
        CREATE MODEL my_model2 WITH (
            model_class = 'dask_ml.cluster.KMeans'
        ) AS (
            SELECT x, y
            FROM timeseries
            LIMIT 100
        )
    """
    )
    c.sql(
        """
        CREATE MODEL my_model3 WITH (
            model_class = 'sklearn.linear_model.SGDClassifier',
            wrap_fit = True,
            target_column = 'target',
            fit_kwargs = ( classes = ARRAY [0, 1] )
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
    """
    )
    expected = pd.DataFrame(["my_model1", "my_model2", "my_model3"], columns=["Models"])
    result: pd.DataFrame = c.sql("SHOW MODELS").compute()
    # test
    pd.testing.assert_frame_equal(expected, result)


def test_wrong_training_or_prediction(c, training_df):
    with pytest.raises(KeyError):
        c.sql(
            """
            SELECT * FROM PREDICT(
            MODEL my_model,
            SELECT x, y FROM timeseries
        )
        """
        )

    with pytest.raises(ValueError):
        c.sql(
            """
            CREATE MODEL my_model WITH (
                target_column = 'target'
            ) AS (
                SELECT x, y, x*y > 0 AS target
                FROM timeseries
                LIMIT 100
            )
        """
        )

    with pytest.raises(ValueError):
        c.sql(
            """
            CREATE MODEL my_model WITH (
                model_class = 'that.is.not.a.python.class',
                target_column = 'target'
            ) AS (
                SELECT x, y, x*y > 0 AS target
                FROM timeseries
                LIMIT 100
            )
        """
        )


def test_correct_argument_passing(c, training_df):
    c.sql(
        """
        CREATE MODEL my_model WITH (
            model_class = 'mock.MagicMock',
            target_column = 'target',
            fit_kwargs = (
                first_arg = 3,
                second_arg = ARRAY [ 1, 2 ],
                third_arg = MAP [ 'a', 1 ],
                forth_arg = MULTISET [ 1, 1, 2, 3 ]
            )
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
    """
    )

    mocked_model, columns = c.schema[c.schema_name].models["my_model"]
    assert list(columns) == ["x", "y"]

    fit_function = mocked_model.fit

    fit_function.assert_called_once()
    call_kwargs = fit_function.call_args.kwargs
    assert call_kwargs == dict(
        first_arg=3, second_arg=[1, 2], third_arg={"a": 1}, forth_arg=set([1, 2, 3])
    )


def test_replace_and_error(c, training_df):
    c.sql(
        """
        CREATE MODEL my_model WITH (
            model_class = 'mock.MagicMock',
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
    """
    )

    first_mock, _ = c.schema[c.schema_name].models["my_model"]

    with pytest.raises(RuntimeError):
        c.sql(
            """
            CREATE MODEL my_model WITH (
                model_class = 'mock.MagicMock',
                target_column = 'target'
            ) AS (
                SELECT x, y, x*y > 0 AS target
                FROM timeseries
                LIMIT 100
            )
        """
        )

    c.sql(
        """
        CREATE MODEL IF NOT EXISTS my_model WITH (
            model_class = 'mock.MagicMock',
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
    """
    )

    assert c.schema[c.schema_name].models["my_model"][0] == first_mock

    c.sql(
        f"""
        CREATE OR REPLACE MODEL my_model WITH (
            model_class = 'mock.MagicMock',
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
    """
    )

    assert c.schema[c.schema_name].models["my_model"][0] != first_mock
    second_mock, _ = c.schema[c.schema_name].models["my_model"]

    c.sql("DROP MODEL my_model")

    c.sql(
        f"""
        CREATE MODEL IF NOT EXISTS my_model WITH (
            model_class = 'mock.MagicMock',
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
    """
    )

    assert c.schema[c.schema_name].models["my_model"][0] != second_mock


def test_drop_model(c, training_df):
    with pytest.raises(RuntimeError):
        c.sql("DROP MODEL my_model")

    c.sql("DROP MODEL IF EXISTS my_model")

    c.sql(
        f"""
        CREATE MODEL IF NOT EXISTS my_model WITH (
            model_class = 'mock.MagicMock',
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
    """
    )

    c.sql("DROP MODEL IF EXISTS my_model")

    assert "my_model" not in c.schema[c.schema_name].models


def test_describe_model(c, training_df):
    c.sql(
        """
        CREATE MODEL ex_describe_model WITH (
            model_class = 'sklearn.ensemble.GradientBoostingClassifier',
            wrap_predict = True,
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
    """
    )

    model, training_columns = c.schema[c.schema_name].models["ex_describe_model"]
    expected_dict = model.get_params()
    expected_dict["training_columns"] = training_columns.tolist()
    # hack for converting model class into string
    expected_series = (
        pd.DataFrame.from_dict(expected_dict, orient="index", columns=["Params"])[
            "Params"
        ]
        .apply(lambda x: str(x))
        .sort_index()
    )
    # test
    result = (
        c.sql("DESCRIBE MODEL ex_describe_model")
        .compute()["Params"]
        .apply(lambda x: str(x))
    )
    pd.testing.assert_series_equal(expected_series, result)

    with pytest.raises(RuntimeError):
        c.sql("DESCRIBE MODEL undefined_model")


def test_export_model(c, training_df, tmpdir):
    with pytest.raises(RuntimeError):
        c.sql(
            """EXPORT MODEL not_available_model with (
                format ='pickle',
                location = '/tmp/model.pkl'
            )"""
        )

    c.sql(
        f"""
        CREATE MODEL IF NOT EXISTS my_model WITH (
            model_class = 'sklearn.ensemble.GradientBoostingClassifier',
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
    """
    )
    # Happy flow
    temporary_file = os.path.join(tmpdir, "pickle_model.pkl")
    c.sql(
        """EXPORT MODEL my_model with (
            format ='pickle',
            location = '{}'
        )""".format(
            temporary_file
        )
    )

    assert (
        pickle.load(open(str(temporary_file), "rb")).__class__.__name__
        == "GradientBoostingClassifier"
    )
    temporary_file = os.path.join(tmpdir, "model.joblib")
    c.sql(
        """EXPORT MODEL my_model with (
            format ='joblib',
            location = '{}'
        )""".format(
            temporary_file
        )
    )

    assert (
        joblib.load(str(temporary_file)).__class__.__name__
        == "GradientBoostingClassifier"
    )

    with pytest.raises(NotImplementedError):
        temporary_dir = os.path.join(tmpdir, "model.onnx")
        c.sql(
            """EXPORT MODEL my_model with (
                format ='onnx',
                location = '{}'
            )""".format(
                temporary_dir
            )
        )


def test_mlflow_export(c, training_df, tmpdir):
    # Test only when mlflow was installed
    mlflow = pytest.importorskip("mlflow", reason="mflow not installed")

    c.sql(
        f"""
        CREATE MODEL IF NOT EXISTS my_model WITH (
            model_class = 'sklearn.ensemble.GradientBoostingClassifier',
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
    """
    )
    temporary_dir = os.path.join(tmpdir, "mlflow")
    c.sql(
        """EXPORT MODEL my_model with (
            format ='mlflow',
            location = '{}'
        )""".format(
            temporary_dir
        )
    )
    # for sklearn compatible model
    assert (
        mlflow.sklearn.load_model(str(temporary_dir)).__class__.__name__
        == "GradientBoostingClassifier"
    )

    # test for non sklearn compatible model
    c.sql(
        f"""
        CREATE MODEL IF NOT EXISTS non_sklearn_model WITH (
            model_class = 'mock.MagicMock',
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
    """
    )
    temporary_dir = os.path.join(tmpdir, "non_sklearn")
    with pytest.raises(NotImplementedError):
        c.sql(
            """EXPORT MODEL non_sklearn_model with (
                format ='mlflow',
                location = '{}'
            )""".format(
                temporary_dir
            )
        )


def test_mlflow_export_xgboost(c, client, training_df, tmpdir):
    # Test only when mlflow & xgboost was installed
    mlflow = pytest.importorskip("mlflow", reason="mflow not installed")
    xgboost = pytest.importorskip("xgboost", reason="xgboost not installed")
    c.sql(
        f"""
        CREATE MODEL IF NOT EXISTS my_model_xgboost WITH (
            model_class = 'xgboost.dask.DaskXGBClassifier',
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
    """
    )
    temporary_dir = os.path.join(tmpdir, "mlflow_xgboost")
    c.sql(
        """EXPORT MODEL my_model_xgboost with (
            format = 'mlflow',
            location = '{}'
        )""".format(
            temporary_dir
        )
    )
    assert (
        mlflow.sklearn.load_model(str(temporary_dir)).__class__.__name__
        == "DaskXGBClassifier"
    )


def test_mlflow_export_lightgbm(c, training_df, tmpdir):
    # Test only when mlflow & lightgbm was installed
    mlflow = pytest.importorskip("mlflow", reason="mflow not installed")
    lightgbm = pytest.importorskip("lightgbm", reason="lightgbm not installed")
    c.sql(
        f"""
        CREATE MODEL IF NOT EXISTS my_model_lightgbm WITH (
            model_class = 'lightgbm.LGBMClassifier',
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
    """
    )
    temporary_dir = os.path.join(tmpdir, "mlflow_lightgbm")
    c.sql(
        """EXPORT MODEL my_model_lightgbm with (
            format = 'mlflow',
            location = '{}'
        )""".format(
            temporary_dir
        )
    )
    assert (
        mlflow.sklearn.load_model(str(temporary_dir)).__class__.__name__
        == "LGBMClassifier"
    )


def test_ml_experiment(c, client, training_df):

    with pytest.raises(
        ValueError,
        match="Parameters must include a 'model_class' " "or 'automl_class' parameter.",
    ):

        c.sql(
            """
        CREATE EXPERIMENT my_exp WITH (
            experiment_class = 'dask_ml.model_selection.GridSearchCV',
            tune_parameters = (n_estimators = ARRAY [16, 32, 2],learning_rate = ARRAY [0.1,0.01,0.001],
                               max_depth = ARRAY [3,4,5,10]),
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
        """
        )
    with pytest.raises(
        ValueError,
        match="Parameters must include a 'experiment_class' "
        "parameter for tuning sklearn.ensemble.GradientBoostingClassifier.",
    ):
        c.sql(
            """
        CREATE EXPERIMENT my_exp WITH (
            model_class = 'sklearn.ensemble.GradientBoostingClassifier',
            tune_parameters = (n_estimators = ARRAY [16, 32, 2],learning_rate = ARRAY [0.1,0.01,0.001],
                               max_depth = ARRAY [3,4,5,10]),
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
        """
        )

    with pytest.raises(
        ValueError,
        match="Can not import model that.is.not.a.python.class. Make sure you spelled "
        "it correctly and have installed all packages.",
    ):
        c.sql(
            """
            CREATE EXPERIMENT IF NOT EXISTS my_exp WITH (
            model_class = 'that.is.not.a.python.class',
            experiment_class = 'dask_ml.model_selection.GridSearchCV',
            tune_parameters = (n_estimators = ARRAY [16, 32, 2],learning_rate = ARRAY [0.1,0.01,0.001],
                               max_depth = ARRAY [3,4,5,10]),
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
        """
        )

    with pytest.raises(
        ValueError,
        match="Can not import tuner that.is.not.a.python.class. Make sure you spelled "
        "it correctly and have installed all packages.",
    ):
        c.sql(
            """
            CREATE EXPERIMENT IF NOT EXISTS my_exp WITH (
            model_class =  'sklearn.ensemble.GradientBoostingClassifier',
            experiment_class = 'that.is.not.a.python.class',
            tune_parameters = (n_estimators = ARRAY [16, 32, 2],learning_rate = ARRAY [0.1,0.01,0.001],
                               max_depth = ARRAY [3,4,5,10]),
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
        """
        )
    with pytest.raises(
        ValueError,
        match="Can not import automl model that.is.not.a.python.class. "
        "Make sure you spelled "
        "it correctly and have installed all packages.",
    ):
        c.sql(
            """
            CREATE EXPERIMENT my_exp64 WITH (
                automl_class = 'that.is.not.a.python.class',
                automl_kwargs = (population_size = 2 ,generations=2,cv=2,n_jobs=-1,use_dask=True,max_eval_time_mins=1),
                target_column = 'target'
            ) AS (
                SELECT x, y, x*y > 0 AS target
                FROM timeseries
                LIMIT 100
            )
            """
        )
    # happy flow
    c.sql(
        """
        CREATE EXPERIMENT my_exp WITH (
        model_class = 'sklearn.ensemble.GradientBoostingClassifier',
        experiment_class = 'dask_ml.model_selection.GridSearchCV',
        tune_parameters = (n_estimators = ARRAY [16, 32, 2],learning_rate = ARRAY [0.1,0.01,0.001],
                           max_depth = ARRAY [3,4,5,10]),
        target_column = 'target'
    ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
        """
    )

    assert "my_exp" in c.schema[c.schema_name].models, "Best model was not registered"

    check_trained_model(c, "my_exp")

    with pytest.raises(RuntimeError):
        # my_exp already exists
        c.sql(
            """
            CREATE EXPERIMENT my_exp WITH (
            model_class = 'sklearn.ensemble.GradientBoostingClassifier',
            experiment_class = 'dask_ml.model_selection.GridSearchCV',
            tune_parameters = (n_estimators = ARRAY [16, 32, 2],learning_rate = ARRAY [0.1,0.01,0.001],
                               max_depth = ARRAY [3,4,5,10]),
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
            """
        )
    c.sql(
        """
        CREATE EXPERIMENT IF NOT EXISTS my_exp WITH (
            model_class = 'sklearn.ensemble.GradientBoostingClassifier',
            experiment_class = 'dask_ml.model_selection.GridSearchCV',
            tune_parameters = (n_estimators = ARRAY [16, 32, 2],learning_rate = ARRAY [0.1,0.01,0.001],
                               max_depth = ARRAY [3,4,5,10]),
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )

        """
    )
    c.sql(
        """
        CREATE OR REPLACE EXPERIMENT my_exp WITH (
            model_class = 'sklearn.ensemble.GradientBoostingClassifier',
            experiment_class = 'dask_ml.model_selection.GridSearchCV',
            tune_parameters = (n_estimators = ARRAY [16, 32, 2],learning_rate = ARRAY [0.1,0.01,0.001],
                               max_depth = ARRAY [3,4,5,10]),
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
        """
    )

    with pytest.raises(
        ValueError,
        match="Unsupervised Algorithm cannot be tuned Automatically,"
        "Consider providing 'target column'",
    ):
        c.sql(
            """
            CREATE EXPERIMENT my_exp1 WITH (
                model_class = 'dask_ml.cluster.KMeans',
                experiment_class = 'dask_ml.model_selection.RandomizedSearchCV',
                tune_parameters = (n_clusters = ARRAY [3,4,16],tol = ARRAY [0.1,0.01,0.001],
                                   max_iter = ARRAY [3,4,5,10])
            ) AS (
                SELECT x, y
                FROM timeseries
                LIMIT 100
            )
            """
        )


def test_experiment_automl_classifier(c, client, training_df):
    tpot = pytest.importorskip("tpot", reason="tpot not installed")
    # currently tested with tpot==
    c.sql(
        """
        CREATE EXPERIMENT my_automl_exp1 WITH (
            automl_class = 'tpot.TPOTClassifier',
            automl_kwargs = (population_size = 2 ,generations=2,cv=2,n_jobs=-1,use_dask=True),
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
        """
    )
    assert (
        "my_automl_exp1" in c.schema[c.schema_name].models
    ), "Best model was not registered"

    check_trained_model(c, "my_automl_exp1")


def test_experiement_automl_regressor(c, client, training_df):
    tpot = pytest.importorskip("tpot", reason="tpot not installed")
    # test regressor
    c.sql(
        """
        CREATE EXPERIMENT my_automl_exp2 WITH (
            automl_class = 'tpot.TPOTRegressor',
            automl_kwargs = (population_size = 2,
            generations=2,
            cv=2,
            n_jobs=-1,
            use_dask=True,
            max_eval_time_mins=1),

            target_column = 'target'
        ) AS (
            SELECT x, y, x*y  AS target
            FROM timeseries
            LIMIT 100
        )
        """
    )
    assert (
        "my_automl_exp2" in c.schema[c.schema_name].models
    ), "Best model was not registered"

    check_trained_model(c, "my_automl_exp2")
