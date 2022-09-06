import logging
import pickle
from typing import TYPE_CHECKING

from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.utils import convert_sql_kwargs

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan

logger = logging.getLogger(__name__)


class ExportModelPlugin(BaseRelPlugin):
    """
     Export a trained model into a file using one of the supported model serialization libraries.

    Sql syntax:
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

        NOTE:
        - Since dask-sql expects fit-predict style model (i.e sklearn compatible model),
            Only sklearn flavoured/sklearn subclassed models are supported as a part of mlflow serialization.
            i.e only mlflow sklearn flavour was used for all the sklearn compatible models.
            for example :
                instead of using xgb.core.Booster consider using xgboost.XGBClassifier
                since later is sklearn compatible
    """

    class_name = "ExportModel"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context"):
        export_model = rel.export_model()

        schema_name, model_name = context.schema_name, export_model.getModelName()
        kwargs = convert_sql_kwargs(export_model.getSQLWithOptions())

        format = kwargs.pop("format", "pickle").lower().strip()
        location = kwargs.pop("location", "tmp.pkl").strip()
        try:
            model, training_columns = context.schema[schema_name].models[model_name]
        except KeyError:
            raise RuntimeError(f"A model with the name {model_name} is not present.")

        logger.info(
            f"Using model serde has {format} and model will be exported to {location}"
        )
        if format in ["pickle", "pkl"]:
            with open(location, "wb") as pkl_file:
                pickle.dump(model, pkl_file, **kwargs)
        elif format == "joblib":
            import joblib

            joblib.dump(model, location, **kwargs)
        elif format == "mlflow":
            try:
                import mlflow
            except ImportError:  # pragma: no cover
                raise ImportError(
                    "For export in the mlflow format, you need to have mlflow installed"
                )
            try:
                import sklearn
            except ImportError:  # pragma: no cover
                sklearn = None
            if sklearn is not None and isinstance(model, sklearn.base.BaseEstimator):
                mlflow.sklearn.save_model(model, location, **kwargs)
            else:
                raise NotImplementedError(
                    "dask-sql supports only sklearn compatible model i.e fit-predict style model"
                )
        elif format == "onnx":
            """
            Need's Columns and their data type for converting
            any model format into Onnx format, and for every framework,
            need to install respective ONNX converters
            """
            # TODO: Add support for Exporting model into ONNX format
            raise NotImplementedError("ONNX format currently not supported")
