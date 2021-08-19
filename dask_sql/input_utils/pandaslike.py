import dask.dataframe as dd
import pandas as pd

try:
    import cudf
except ImportError:  # pragma: no cover
    cudf = None

from dask_sql.input_utils.base import BaseInputPlugin


class PandasLikeInputPlugin(BaseInputPlugin):
    """Input Plugin for Pandas DataFrames, which get converted to dask DataFrames"""

    def is_correct_input(
        self, input_item, table_name: str, format: str = None, **kwargs
    ):
        is_cudf_type = cudf and isinstance(input_item, cudf.DataFrame)
        return is_cudf_type or isinstance(input_item, pd.DataFrame) or format == "dask"

    def to_dc(self, input_item, table_name: str, format: str = None, **kwargs):
        npartitions = kwargs.pop("npartitions", 1)
        return dd.from_pandas(input_item, npartitions=npartitions, **kwargs)
