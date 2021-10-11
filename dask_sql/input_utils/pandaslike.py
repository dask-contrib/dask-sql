import dask.dataframe as dd
import pandas as pd

from dask_sql.input_utils.base import BaseInputPlugin

try:
    import cudf
    import dask_cudf
except ImportError:
    dask_cudf = None


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
            if not dask_cudf:
                raise ModuleNotFoundError(
                    "Setting `gpu=True` for table creation requires dask_cudf"
                )

            if isinstance(input_item, pd.DataFrame):
                return dask_cudf.from_cudf(
                    cudf.from_pandas(input_item), npartitions=npartitions, **kwargs,
                )
            else:
                return dask_cudf.from_cudf(
                    input_item, npartitions=npartitions, **kwargs,
                )
        else:
            return dd.from_pandas(input_item, npartitions=npartitions, **kwargs)
