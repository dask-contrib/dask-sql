import dask.dataframe as dd
import pandas as pd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.utils import get_model_from_compound_identifier


class ShowModelParamsPlugin(BaseRelPlugin):
    """
    Show all Params used to train a given model and training columns.
    The SQL is:

        DESCRIBE MODEL <model_name>

    The result is also a table, although it is created on the fly.
    """

    class_name = "com.dask.sql.parser.SqlShowModelParams"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        components = list(map(str, sql.getTable().names))
        model, training_columns = get_model_from_compound_identifier(
            context, components
        )
        model_params = model.get_params()
        model_params["training_columns"] = training_columns.tolist()
        df = pd.DataFrame.from_dict(model_params, orient="index", columns=["Params"])
        cc = ColumnContainer(df.columns)
        dc = DataContainer(dd.from_pandas(df, npartitions=1), cc)
        return dc
