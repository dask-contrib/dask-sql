import dask.dataframe as dd
import pandas as pd

from dask_sql.input_utils.base import BaseInputPlugin

try:
    import cudf
except ImportError:
    cudf = None


class PandasLikeInputPlugin(BaseInputPlugin):
    """Input Plugin for Pandas Like DataFrames, which get converted to dask DataFrames"""

    def is_correct_input(
        self, input_item, table_name: str, format: str = None, **kwargs
    ):
        is_cudf_type = cudf and isinstance(input_item, cudf.DataFrame)
        return is_cudf_type or isinstance(input_item, pd.DataFrame) or format == "dask"

    def to_dc(
        self,
        input_item,
        table_name: str,
        format: str = None,
        gpu: bool = False,
        **kwargs,
    ):
        npartitions = kwargs.pop("npartitions", 1)
        if gpu:  # pragma: no cover
            if not cudf:
                raise ModuleNotFoundError(
                    "Setting `gpu=True` for table creation requires cudf"
                )

            if isinstance(input_item, pd.DataFrame):
                input_item = cudf.from_pandas(input_item)

        return dd.from_pandas(input_item, npartitions=npartitions, **kwargs)
