import dask.dataframe as dd
import pandas as pd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin


class ShowModelParamsPlugin(BaseRelPlugin):
    """
    Show all Params used to train a given model along with the columns
    used for training.
    The SQL is:

        DESCRIBE MODEL <model_name>

    The result is also a table, although it is created on the fly.
    """

    class_name = "com.dask.sql.parser.SqlShowModelParams"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        schema_name, model_name = context.fqn(sql.getModelName().getIdentifier())

        if model_name not in context.schema[schema_name].models:
            raise RuntimeError(f"A model with the name {model_name} is not present.")

        model, training_columns = context.schema[schema_name].models[model_name]

        model_params = model.get_params()
        model_params["training_columns"] = training_columns.tolist()

        df = pd.DataFrame.from_dict(model_params, orient="index", columns=["Params"])
        cc = ColumnContainer(df.columns)
        dc = DataContainer(dd.from_pandas(df, npartitions=1), cc)
        return dc
