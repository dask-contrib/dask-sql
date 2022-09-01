import dask.dataframe as dd
import pandas as pd

from dask_sql.input_utils.base import BaseInputPlugin


class PandasLikeInputPlugin(BaseInputPlugin):
    """Input Plugin for Pandas Like DataFrames, which get converted to dask DataFrames"""

    def is_correct_input(
        self, input_item, table_name: str, format: str = None, **kwargs
    ):
        return (
            dd.utils.is_dataframe_like(input_item)
            and not isinstance(input_item, dd.DataFrame)
        ) or format == "dask"

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
            try:
                import cudf
            except ImportError:
                raise ModuleNotFoundError(
                    "Setting `gpu=True` for table creation requires cudf"
                )

            if isinstance(input_item, pd.DataFrame):
                input_item = cudf.from_pandas(input_item)

        return dd.from_pandas(input_item, npartitions=npartitions, **kwargs)
