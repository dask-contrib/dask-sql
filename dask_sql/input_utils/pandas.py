import dask.dataframe as dd
import pandas as pd

from dask_sql.input_utils.base import BaseInputPlugin


class PandasInputPlugin(BaseInputPlugin):
    """Input Plugin for Pandas DataFrames, which get converted to dask DataFrames"""

    def is_correct_input(
        self, input_item, table_name: str, format: str = None, **kwargs
    ):
        return isinstance(input_item, pd.DataFrame) or format == "dask"

    def to_dc(
        self,
        input_item,
        table_name: str,
        format: str = None,
        gpu: bool = False,
        **kwargs,
    ):
        npartitions = kwargs.pop("npartitions", 1)
        if gpu:
            import dask_cudf

            return dask_cudf.from_cudf(input_item, npartitions=npartitions, **kwargs)
        else:
            return dd.from_pandas(input_item, npartitions=npartitions, **kwargs)
