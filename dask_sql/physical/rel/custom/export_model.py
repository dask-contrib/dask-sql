import pickle

import joblib
import sklearn

from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.utils import convert_sql_kwargs

try:
    import xgboost
except ImportError:
    xgboost = None
try:
    import lightgbm
except ImportError:
    lightgbm = None


class ExportModelPlugin(BaseRelPlugin):
    """
     Export a trained model into a file using one of th supported model serialization libraries.

    Sql:
        EXPORT MODEL <model_name> WTIH (
            format = "pickle",
            location = "model.pkl"
        )
    1. Most of the machine learning model framework support pickle as a serialization format
        for example:
            sklearn
            Pytorch
    2. To export a universal (framework agnostic) model, use the mlflow (https://mlflow.org/) format
        - mlflow is a framework, which supports different flavors of model serialization, implemented
        for different ML libraries like xgboost,catboost,lightgbm etc.
        - A mlflow model is a self-contained artifact, which contains everything you need for
        loading the model - without import errors
        - To reproduce the environment, conda.yaml files are produced while saving the
        model and stored as part of the mlflow model

    TODO:
        Add support for Exporting model into ONNX format
    """

    class_name = "com.dask.sql.parser.SqlExportModel"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ):

        model_name = str(sql.getModelName().getIdentifier())
        kwargs = convert_sql_kwargs(sql.getKwargs())
        format = kwargs.pop("format", "pickle").lower()
        location = kwargs.pop("location", "tmp.pkl")
        try:
            model, training_columns = context.models[model_name]
        except KeyError:
            raise RuntimeError(f"A model with the name {model_name} is not present.")

        if format in ["pickle", "pkl"]:
            with open(location, "wb") as pkl_file:
                pickle.dump(model, pkl_file, **kwargs)
        elif format == "joblib":
            joblib.dump(model, location, **kwargs)

        elif format == "mlflow":
            try:
                import mlflow
            except ImportError:
                raise ImportError(
                    f"For export in the mlflow format, you need to have mlflow installed"
                )
            if isinstance(model, sklearn.base.BaseEstimator):
                mlflow.sklearn.save_model(model, location, **kwargs)
            elif xgboost and isinstance(model, xgboost.core.Booster):
                mlflow.xgboost.save_model(model, location, **kwargs)
            elif lightgbm and isinstance(model, lightgbm.basic.Booster):
                mlflow.lightgbm.save_model(model, location, **kwargs)
            else:
                raise NotImplementedError(
                    f"mlflow export of type  {model.__class__} is  not implemented (yet)"
                )
        elif format == "onnx":
            """
            Need's Columns and their data type for converting
            any model format into Onnx format, and for every framework,
            need to install respective ONNX converters
            """

            raise NotImplementedError("ONNX format currently not supported")
