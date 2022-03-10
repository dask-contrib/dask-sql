import logging
import pickle
from typing import TYPE_CHECKING

from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.utils import convert_sql_kwargs

if TYPE_CHECKING:
    import dask_sql
    from dask_sql.java import org

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

    class_name = "com.predibase.pql.parser.SqlExportModel"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ):

        schema_name, model_name = context.fqn(sql.getModel().getName())

        location = sql.getOutputUri()
        format = sql.getFramework()

        logger.info(
            f"Using model serde has {format} and model will be exported to {location}"
        )

        # TODO: Add exporet support
        raise NotImplementedError("PQL Export not implemented yet")
