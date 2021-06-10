import pickle

import joblib
import sklearn

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.utils import convert_sql_kwargs


class SqlExportModel(BaseRelPlugin):
    """
    Export a trained model into a file using supported model serilization or model serDe.

    Sql:
        EXPORT MODEL <model_name> WTIH (
            model_serde = "pickle",
            location = "model.pkl"
        )
    1. Most of the framework support pickle as a serilization
        example:
            sklearn
            Dask ML
            Pytorch
    2. To Export a Universal (Framework agnostic model) use mlflow format
        - A mlflow is a framework which supports different flavours of model serilization, implemented
        for different ML libraries.
        - Mlflow model is a self contained directory full of files which helps to load the
        saved model without any environment Error
        - To reproduce the environment conda.yaml files are produced while saving the model
        Files saved as part of mlflow model
            - conda.yaml (packages required to import saved model)
            - MLmodel (A artifact about model which contains following)
                - mlflow function which can load this saved model
                - model was saved timestmap  etc..
            - model : a ml model itself
                - sklearn - model.pkl
                - xgboost - model.xgb
                - lightgbm - model.lgb
    3. Export into ONNX (open standard ) -
         - Needs input column names and their type to serilizes the model

    """

    class_name = "com.dask.sql.parser.SqlExportModel"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ):

        model_name = str(sql.getModelName().getIdentifier())
        kwargs = convert_sql_kwargs(sql.getKwargs())
        model_serde = kwargs.pop("model_serde", "pickle")
        location = kwargs.pop("location", "tmp.pkl")
        try:
            model, training_columns = context.models[model_name]
        except KeyError:
            raise RuntimeError(f"A model with the name {model_name} is not present.")

        if model_serde.lower() == "pickle" or model_serde.lower() == "pkl":
            with open(location, "wb") as pkl_file:
                pickle.dump(model, pkl_file)
        elif model_serde.lower() == "joblib":
            joblib.dump(model, location)
        # somewhat Experimental
        elif model_serde.lower() == "mlflow":
            try:
                import lightgbm
                import mlflow
                import xgboost
            except ImportError as impError:
                raise RuntimeError(
                    f"Please install required packages - Failed with {impError}"
                )
            if isinstance(model, sklearn.base.BaseEstimator):
                mlflow.sklearn.save_model(model, location, **kwargs)
            elif isinstance(model, xgboost.core.Booster):
                mlflow.xgboost.save_model(model, location, **kwargs)
            elif isinstance(model, lightgbm.basic.Booster):
                mlflow.lightgbm.save_model(model, location, **kwargs)
            else:
                raise NotImplementedError(
                    f"Model {model.__class__} was not implemented"
                )
        elif model_serde.lower() == "onnx":
            """
            Needs Columns and their data type for converting sklearn model into Onnx model

            example:
            from skl2onnx import convert_sklearn
            from skl2onnx.common.data_types import FloatTensorType
            initial_type = [('float_input', FloatTensorType([None, 4]))]

            onx = convert_sklearn(model, initial_types=initial_type)
            with open("rf_iris.onnx", "wb") as f:
                f.write(onx.SerializeToString()
            """

            raise NotImplementedError("ONNX format currently not supported")
