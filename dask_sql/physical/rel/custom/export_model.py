import pickle

import dask.dataframe as dd
import pandas as pd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.utils import get_model_from_compound_identifier


class SqlExportModel(BaseRelPlugin):
    """
    Show all Params used to train a given model and training columns.
    The SQL is:

        DESCRIBE MODEL <model_name>

    The result is also a table, although it is created on the fly.
    """

    class_name = "com.dask.sql.parser.SqlExportModel"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        components = list(map(str, sql.getTable().names))
        print(components)

        modelName = components[-1]
        fileName = str(sql.getFileName())
        print(fileName)
        print(context.models)
        try:
            model, _ = context.models[modelName]
            with open(fileName, "wb") as pkl_file:
                pickle.dump(model, pkl_file)
        except KeyError:
            raise AttributeError(f"Model {modelName} is not defined.")
        except Exception as e:
            raise e

        # model, training_columns = get_model_from_compound_identifier(
        #     context, components
        # )

        # model_params = model.get_params()
        # model_params["training_columns"] = training_columns.tolist()
        # df = pd.DataFrame.from_dict(model_params, orient="index", columns=["Params"])
        # cc = ColumnContainer(df.columns)
        # dc = DataContainer(dd.from_pandas(df, npartitions=1), cc)
        return
