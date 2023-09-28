import os
import pickle
import sys

import joblib
import pandas as pd
import pytest

from tests.utils import assert_eq

try:
    import cuml
    import dask_cudf
    import xgboost
except ImportError:
    cuml = None
    xgboost = None
    dask_cudf = None


def check_trained_model(c, model_name="my_model", df_name="timeseries"):
    sql = f"""
    SELECT * FROM PREDICT(
        MODEL {model_name},
        SELECT x, y FROM {df_name}
    )
    """

    tables_before = c.schema["root"].tables.keys()
    result_df = c.sql(sql).compute()

    # assert that there are no additional tables in context from prediction
    assert tables_before == c.schema["root"].tables.keys()
    assert "target" in result_df.columns
    assert len(result_df["target"]) > 0


@pytest.mark.parametrize(
    "gpu_client", [False, pytest.param(True, marks=pytest.mark.gpu)], indirect=True
)
def test_training_and_prediction(c, gpu_client):
    gpu = "CUDA" in str(gpu_client.cluster)
    timeseries = "gpu_timeseries" if gpu else "timeseries"

    # cuML does not have a GradientBoostingClassifier
    if not gpu:
        c.sql(
            """
            CREATE MODEL my_model WITH (
                model_class = 'GradientBoostingClassifier',
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

        c.sql(
            f"""
            CREATE OR REPLACE MODEL my_model WITH (
                model_class = 'LogisticRegression',
                wrap_predict = True,
                wrap_fit = False,
                target_column = 'target'
            ) AS (
                SELECT x, y, x*y > 0 AS target
                FROM {timeseries}
            )
        """
        )
        check_trained_model(c, df_name=timeseries)

    c.sql(
        f"""
        CREATE OR REPLACE MODEL my_model WITH (
            model_class = 'LinearRegression',
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y AS target
            FROM {timeseries}
        )
    """
    )
    check_trained_model(c, df_name=timeseries)


@pytest.mark.flaky(
    reruns=4, condition="sys.version_info >= (3, 9) and sys.platform == 'darwin'"
)
@pytest.mark.xfail(
    sys.platform == "win32",
    reason="'xgboost.core.XGBoostError: Failed to poll' on Windows only",
)
@pytest.mark.parametrize(
    "gpu_client", [False, pytest.param(True, marks=pytest.mark.gpu)], indirect=True
)
def test_xgboost_training_prediction(c, gpu_client):
    gpu = "CUDA" in str(gpu_client.cluster)
    timeseries = "gpu_timeseries" if gpu else "timeseries"

    # TODO: XGBClassifiers error on GPU
    if not gpu:
        c.sql(
            """
        CREATE OR REPLACE MODEL my_model WITH (
            model_class = 'DaskXGBClassifier',
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0  AS target
            FROM timeseries
            LIMIT 100
        )
        """
        )
        check_trained_model(c)

        c.sql(
            """
        CREATE OR REPLACE MODEL my_model WITH (
            model_class = 'XGBClassifier',
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0  AS target
            FROM timeseries
            LIMIT 100
        )
        """
        )
        check_trained_model(c)

    # For GPU tests, set tree_method = 'gpu_hist'
    tree_method = "gpu_hist" if gpu else "hist"

    c.sql(
        f"""
    CREATE OR REPLACE MODEL my_model WITH (
        model_class = 'DaskXGBRegressor',
        target_column = 'target',
        tree_method = '{tree_method}'
    ) AS (
        SELECT x, y, x*y  AS target
        FROM {timeseries}
    )
    """
    )
    check_trained_model(c, df_name=timeseries)

    c.sql(
        f"""
    CREATE OR REPLACE MODEL my_model WITH (
        model_class = 'XGBRegressor',
        wrap_predict = True,
        target_column = 'target',
        tree_method = '{tree_method}'
    ) AS (
        SELECT x, y, x*y  AS target
        FROM {timeseries}
    )
    """
    )
    check_trained_model(c, df_name=timeseries)


@pytest.mark.parametrize(
    "gpu_client", [False, pytest.param(True, marks=pytest.mark.gpu)], indirect=True
)
def test_clustering_and_prediction(c, gpu_client):
    gpu = "CUDA" in str(gpu_client.cluster)
    timeseries = "gpu_timeseries" if gpu else "timeseries"

    c.sql(
        f"""
        CREATE MODEL my_model WITH (
            model_class = 'KMeans'
        ) AS (
            SELECT x, y
            FROM {timeseries}
            LIMIT 100
        )
    """
    )
    check_trained_model(c, df_name=timeseries)


def test_create_model_with_prediction(c):
    c.sql(
        """
        CREATE MODEL my_model1 WITH (
            model_class = 'GradientBoostingClassifier',
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
            model_class = 'GradientBoostingClassifier',
            wrap_predict = True,
            target_column = 'target'
        ) AS (
            SELECT * FROM PREDICT (
                MODEL my_model1,
                SELECT x, y FROM timeseries LIMIT 100
            )
        )
    """
    )

    check_trained_model(c, "my_model2")


def test_iterative_and_prediction(c):
    c.sql(
        """
        CREATE MODEL my_model WITH (
            model_class = 'SGDClassifier',
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


def test_show_models(c):
    c.sql(
        """
        CREATE MODEL my_model1 WITH (
            model_class = 'GradientBoostingClassifier',
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
            model_class = 'KMeans'
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
            model_class = 'SGDClassifier',
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

    result = c.sql("SHOW MODELS")
    expected = pd.DataFrame(["my_model1", "my_model2", "my_model3"], columns=["Models"])

    assert_eq(result, expected)


def test_wrong_training_or_prediction(c):
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

    with pytest.raises(ImportError):
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


def test_correct_argument_passing(c):
    c.sql(
        """
        CREATE MODEL my_model WITH (
            model_class = 'mock.MagicMock',
            target_column = 'target',
            fit_kwargs = (
                single_quoted_string = 'hello',
                double_quoted_string = "hi",
                integer = -300,
                float = 23.45,
                boolean = False,
                array = ARRAY [ 1, 2 ],
                dict = MAP [ 'a', 1 ],
                set = MULTISET [ 1, 1, 2, 3 ]
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
        single_quoted_string="hello",
        double_quoted_string="hi",
        integer=-300,
        float=23.45,
        boolean=False,
        array=[1, 2],
        dict={"a": 1},
        set=set([1, 2, 3]),
    )


def test_replace_and_error(c):
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
        """
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

    assert c.schema[c.schema_name].models["my_model"][0] != second_mock


def test_drop_model(c):
    with pytest.raises(RuntimeError):
        c.sql("DROP MODEL my_model")

    c.sql("DROP MODEL IF EXISTS my_model")

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

    c.sql("DROP MODEL IF EXISTS my_model")

    assert "my_model" not in c.schema[c.schema_name].models


def test_describe_model(c):
    c.sql(
        """
        CREATE MODEL ex_describe_model WITH (
            model_class = 'GradientBoostingClassifier',
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
    result = c.sql("DESCRIBE MODEL ex_describe_model")["Params"].apply(lambda x: str(x))

    assert_eq(expected_series, result)

    with pytest.raises(RuntimeError):
        c.sql("DESCRIBE MODEL undefined_model")


def test_export_model(c, tmpdir):
    with pytest.raises(RuntimeError):
        c.sql(
            """EXPORT MODEL not_available_model with (
                format ='pickle',
                location = '/tmp/model.pkl'
            )"""
        )

    c.sql(
        """
        CREATE MODEL IF NOT EXISTS my_model WITH (
            model_class = 'GradientBoostingClassifier',
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
        pickle.load(open(str(temporary_file), "rb")).estimator.__class__.__name__
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
        joblib.load(str(temporary_file)).estimator.__class__.__name__
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


def test_mlflow_export(c, tmpdir):
    # Test only when mlflow was installed
    mlflow = pytest.importorskip("mlflow", reason="mlflow not installed")

    c.sql(
        """
        CREATE MODEL IF NOT EXISTS my_model WITH (
            model_class = 'GradientBoostingClassifier',
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
        mlflow.sklearn.load_model(str(temporary_dir)).estimator.__class__.__name__
        == "GradientBoostingClassifier"
    )

    # test for non sklearn compatible model
    c.sql(
        """
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


@pytest.mark.flaky(
    reruns=4, condition="sys.version_info >= (3, 9) and sys.platform == 'darwin'"
)
def test_mlflow_export_xgboost(c, client, tmpdir):
    # Test only when mlflow & xgboost was installed
    mlflow = pytest.importorskip("mlflow", reason="mlflow not installed")
    xgboost = pytest.importorskip("xgboost", reason="xgboost not installed")

    c.sql(
        """
        CREATE MODEL IF NOT EXISTS my_model_xgboost WITH (
            model_class = 'DaskXGBClassifier',
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


def test_mlflow_export_lightgbm(c, tmpdir):
    # Test only when mlflow & lightgbm was installed
    mlflow = pytest.importorskip("mlflow", reason="mlflow not installed")
    lightgbm = pytest.importorskip("lightgbm", reason="lightgbm not installed")

    c.sql(
        """
        CREATE MODEL IF NOT EXISTS my_model_lightgbm WITH (
            model_class = 'LGBMClassifier',
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


def test_ml_experiment(c, client):
    with pytest.raises(
        ValueError,
        match="Parameters must include a 'model_class' " "or 'automl_class' parameter.",
    ):
        c.sql(
            """
        CREATE EXPERIMENT my_exp WITH (
            experiment_class = 'GridSearchCV',
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
        "parameter for tuning GradientBoostingClassifier.",
    ):
        c.sql(
            """
        CREATE EXPERIMENT my_exp WITH (
            model_class = 'GradientBoostingClassifier',
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
            experiment_class = 'GridSearchCV',
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
            model_class =  'GradientBoostingClassifier',
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
                automl_kwargs = (
                    population_size = 2,
                    generations = 2,
                    cv = 2,
                    n_jobs = -1,
                    use_dask = True,
                    max_eval_time_mins = 1
                ),
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
        model_class = 'GradientBoostingClassifier',
        experiment_class = 'GridSearchCV',
        tune_parameters = (n_estimators = ARRAY [16, 32, 2],learning_rate = ARRAY [0.1,0.01,0.001],
                           max_depth = ARRAY [3,4,5,10]),
        experiment_kwargs = (n_jobs = -1),
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
            model_class = 'GradientBoostingClassifier',
            experiment_class = 'GridSearchCV',
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
            model_class = 'GradientBoostingClassifier',
            experiment_class = 'GridSearchCV',
            tune_parameters = (n_estimators = ARRAY [16, 32, 2],learning_rate = ARRAY [0.1,0.01,0.001],
                               max_depth = ARRAY [3,4,5,10]),
            experiment_kwargs = (n_jobs = -1),
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
            model_class = 'GradientBoostingClassifier',
            experiment_class = 'GridSearchCV',
            tune_parameters = (n_estimators = ARRAY [16, 32, 2],learning_rate = ARRAY [0.1,0.01,0.001],
                               max_depth = ARRAY [3,4,5,10]),
            experiment_kwargs = (n_jobs = -1),
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
                model_class = 'KMeans',
                experiment_class = 'RandomizedSearchCV',
                tune_parameters = (n_clusters = ARRAY [3,4,16],tol = ARRAY [0.1,0.01,0.001],
                                   max_iter = ARRAY [3,4,5,10])
            ) AS (
                SELECT x, y
                FROM timeseries
                LIMIT 100
            )
            """
        )


def test_experiment_automl_classifier(c, client):
    tpot = pytest.importorskip("tpot", reason="tpot not installed")

    # currently tested with tpot==
    c.sql(
        """
        CREATE EXPERIMENT my_automl_exp1 WITH (
            automl_class = 'tpot.TPOTClassifier',
            automl_kwargs = (population_size=2, generations=2, cv=2, n_jobs=-1),
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


def test_experiment_automl_regressor(c, client):
    tpot = pytest.importorskip("tpot", reason="tpot not installed")

    # test regressor
    c.sql(
        """
        CREATE EXPERIMENT my_automl_exp2 WITH (
            automl_class = 'tpot.TPOTRegressor',
            automl_kwargs = (population_size=2,
            generations=2,
            cv=2,
            n_jobs=-1,
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


def test_predict_with_nullable_types(c):
    df = pd.DataFrame(
        {
            "rough_day_of_year": [0, 1, 2, 3],
            "prev_day_inches_rained": [0.0, 1.0, 2.0, 3.0],
            "rained": [False, False, False, True],
        }
    )
    c.create_table("train_set", df)

    model_class = "'LogisticRegression'"

    c.sql(
        f"""
        CREATE OR REPLACE MODEL model WITH (
            model_class = {model_class},
            wrap_predict = True,
            wrap_fit = False,
            target_column = 'rained'
        ) AS (
            SELECT *
            FROM train_set
        )
        """
    )

    expected = c.sql(
        """
        SELECT * FROM PREDICT(
            MODEL model,
            SELECT * FROM train_set
        )
        """
    )

    df = pd.DataFrame(
        {
            "rough_day_of_year": pd.Series([0, 1, 2, 3], dtype="Int32"),
            "prev_day_inches_rained": pd.Series([0.0, 1.0, 2.0, 3.0], dtype="Float32"),
            "rained": pd.Series([False, False, False, True]),
        }
    )
    c.create_table("train_set", df)

    c.sql(
        f"""
        CREATE OR REPLACE MODEL model WITH (
            model_class = {model_class},
            wrap_predict = True,
            wrap_fit = False,
            target_column = 'rained'
        ) AS (
            SELECT *
            FROM train_set
        )
        """
    )

    result = c.sql(
        """
        SELECT * FROM PREDICT(
            MODEL model,
            SELECT * FROM train_set
        )
        """
    )

    assert_eq(
        expected,
        result,
        check_dtype=False,
    )


def test_predict_with_limit_offset(c):
    c.sql(
        """
        CREATE MODEL my_model WITH (
            model_class = 'GradientBoostingClassifier',
            wrap_predict = True,
            target_column = 'target'
        ) AS (
            SELECT x, y, x*y > 0 AS target
            FROM timeseries
            LIMIT 100
        )
    """
    )

    res = c.sql(
        """
        SELECT * FROM PREDICT (
            MODEL my_model,
            SELECT x, y FROM timeseries LIMIT 100 OFFSET 100
        )
    """
    )

    res.compute()
