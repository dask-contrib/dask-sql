from collections import namedtuple
from typing import Any, Dict, List, Tuple, Union

import dask.dataframe as dd
import pandas as pd

ColumnType = Union[str, int]

FunctionDescription = namedtuple(
    "FunctionDescription", ["name", "parameters", "return_type", "aggregation"]
)


class ColumnContainer:
    # Forward declaration
    pass


class ColumnContainer:
    """
    Helper class to store a list of columns,
    which do not necessarily be the ones of the dask dataframe.
    Instead, the container also stores a mapping from "frontend"
    columns (columns with the names and order expected by SQL)
    to "backend" columns (the real column names used by dask)
    to prevent unnecessary renames.
    """

    def __init__(
        self,
        frontend_columns: List[str],
        frontend_backend_mapping: Union[Dict[str, ColumnType], None] = None,
    ):
        assert all(
            isinstance(col, str) for col in frontend_columns
        ), "All frontend columns need to be of string type"
        self._frontend_columns = list(frontend_columns)
        if frontend_backend_mapping is None:
            self._frontend_backend_mapping = {
                col: col for col in self._frontend_columns
            }
        else:
            self._frontend_backend_mapping = frontend_backend_mapping

    def _copy(self) -> ColumnContainer:
        """
        Internal function to copy this container
        """
        return ColumnContainer(
            self._frontend_columns.copy(), self._frontend_backend_mapping.copy()
        )

    def limit_to(self, fields: List[str]) -> ColumnContainer:
        """
        Create a new ColumnContainer, which has frontend columns
        limited to only the ones given as parameter.
        Also uses the order of these as the new column order.
        """
        if not fields:
            return self  # pragma: no cover

        assert all(f in self._frontend_backend_mapping for f in fields)
        cc = self._copy()
        cc._frontend_columns = [str(x) for x in fields]
        return cc

    def rename(self, columns: Dict[str, str]) -> ColumnContainer:
        """
        Return a new ColumnContainer where the frontend columns
        are renamed according to the given mapping.
        Columns not present in the mapping are not touched,
        the order is preserved.
        """
        cc = self._copy()
        for column_from, column_to in columns.items():
            backend_column = self._frontend_backend_mapping[str(column_from)]
            cc._frontend_backend_mapping[str(column_to)] = backend_column

        cc._frontend_columns = [
            str(columns[col]) if col in columns else col
            for col in self._frontend_columns
        ]

        return cc

    def mapping(self) -> List[Tuple[str, ColumnType]]:
        """
        The mapping from frontend columns to backend columns.
        """
        return list(self._frontend_backend_mapping.items())

    @property
    def columns(self) -> List[str]:
        """
        The stored frontend columns in the correct order
        """
        return self._frontend_columns.copy()

    def add(
        self, frontend_column: str, backend_column: Union[str, None] = None
    ) -> ColumnContainer:
        """
        Return a new ColumnContainer with the
        given column added.
        The column is added at the last position in the column list.
        """
        cc = self._copy()

        frontend_column = str(frontend_column)

        cc._frontend_backend_mapping[frontend_column] = str(
            backend_column or frontend_column
        )
        if frontend_column not in cc._frontend_columns:
            cc._frontend_columns.append(frontend_column)

        return cc

    def get_backend_by_frontend_index(self, index: int) -> str:
        """
        Get back the dask column, which is referenced by the
        frontend (SQL) column with the given index.
        """
        frontend_column = self._frontend_columns[index]
        backend_column = self._frontend_backend_mapping[frontend_column]
        return backend_column

    def get_backend_by_frontend_name(self, column: str) -> str:
        """
        Get back the dask column, which is referenced by the
        frontend (SQL) column with the given name.
        """
        backend_column = self._frontend_backend_mapping[column]
        return backend_column

    def make_unique(self, prefix="col"):
        """
        Make sure we have unique column names by calling each column

            <prefix>_<number>

        where <number> is the column index.
        """
        return self.rename(
            columns={str(col): f"{prefix}_{i}" for i, col in enumerate(self.columns)}
        )


class DataContainer:
    """
    In SQL, every column operation or reference is done via
    the column index. Some dask operations, such as grouping,
    joining or concatenating preserve the columns in a different
    order than SQL would expect.
    However, we do not want to change the column data itself
    all the time (because this would lead to computational overhead),
    but still would like to keep the columns accessible by name and index.
    For this, we add an additional `ColumnContainer` to each dataframe,
    which does all the column mapping between "frontend"
    (what SQL expects, also in the correct order)
    and "backend" (what dask has).
    """

    def __init__(self, df: dd.DataFrame, column_container: ColumnContainer):
        self.df = df
        self.column_container = column_container

    def assign(self) -> dd.DataFrame:
        """
        Combine the column mapping with the actual data and return
        a dataframe which has the the columns specified in the
        stored ColumnContainer.
        """
        df = self.df[
            [
                self.column_container._frontend_backend_mapping[out_col]
                for out_col in self.column_container.columns
            ]
        ]
        df.columns = self.column_container.columns

        return df


class UDF:
    def __init__(self, func, row_udf: bool, return_type=None):
        """
        Helper class that handles different types of UDFs and manages
        how they should be mapped to dask operations. Two versions of
        UDFs are supported - when `row_udf=False`, the UDF is treated
        as expecting series-like objects as arguments and will simply
        run those through the function. When `row_udf=True` a row udf
        is expected and should be written to expect a dictlike object
        containing scalars
        """
        self.row_udf = row_udf
        self.func = func

        if return_type is None:
            # These UDFs go through apply and without providing
            # a return type, dask will attempt to guess it, and
            # dask might be wrong.
            raise ValueError("Return type must be provided")
        self.meta = (None, return_type)

    def __call__(self, *args, **kwargs):
        if self.row_udf:
            column_args = []
            scalar_args = []
            for operand in args:
                if isinstance(operand, dd.Series):
                    column_args.append(operand)
                else:
                    scalar_args.append(operand)
            df = column_args[0].to_frame()
            for col in column_args[1:]:
                df[col.name] = col
            result = df.apply(
                self.func, axis=1, args=tuple(scalar_args), meta=self.meta
            ).astype(self.meta[1])
        else:
            result = self.func(*args, **kwargs)
        return result

    def __eq__(self, other):
        if isinstance(other, UDF):
            return self.func == other.func and self.row_udf == other.row_udf
        return NotImplemented

    def __hash__(self):
        return (self.func, self.row_udf).__hash__()


class Statistics:
    """
    Statistics are used during the cost-based optimization.
    Currently, only the row count is supported, more
    properties might follow. It needs to be provided by the user.
    """

    def __init__(self, row_count: int) -> None:
        self.row_count = row_count


class SchemaContainer:
    def __init__(self, name: str):
        self.__name__ = name
        self.tables: Dict[str, DataContainer] = {}
        self.statistics: Dict[str, Statistics] = {}
        self.experiments: Dict[str, pd.DataFrame] = {}
        self.models: Dict[str, Tuple[Any, List[str]]] = {}
        self.functions: Dict[str, UDF] = {}
        self.function_lists: List[FunctionDescription] = []
        self.config: ConfigContainer = ConfigContainer()


class ConfigContainer:
    """
    Helper class that contains configuration options required for specific operations
    Configurations are stored in a dictionary where keys strings are delimited by `.`
    for easier nested access of multiple configurations
    Example:
        Dask groupby aggregate operations can be configured via with the `split_out` option
        to determine number of output partitions or the `split_every` option to determine
        the number of partitions used during the groupby tree reduction step.
    """

    def __init__(self):
        self.config_dict = {
            # Do not set defaults here unless needed
            # This mantains the list of configuration options supported that can be set
            # "dask.groupby.aggregate.split_out": 1,
            # "dask.groupby.aggregate.split_every": None,
        }

    def set_config(self, config_options: Union[Tuple[str, Any], Dict[str, Any]]):
        """
        Accepts either a tuple of (config, val) or a dictionary containing multiple
        {config1: val1, config2: val2} pairs and updates the schema config with these values
        """
        if isinstance(config_options, tuple):
            config_options = [config_options]
        self.config_dict.update(config_options)

    def drop_config(self, config_strs: Union[str, List[str]]):
        if isinstance(config_strs, str):
            config_strs = [config_strs]
        for config_key in config_strs:
            self.config_dict.pop(config_key)

    def get_config_by_prefix(self, config_prefix: str):
        """
        Returns all configuration options matching the prefix in `config_prefix`

        Example:
            .. code-block:: python

                from dask_sql.datacontainer import ConfigContainer

                sql_config = ConfigContainer()
                sql_config.set_config(
                    {
                     "dask.groupby.aggregate.split_out":1,
                     "dask.groupby.aggregate.split_every": 1,
                     "dask.sort.persist": True,
                    }
                )

                sql_config.get_config_by_prefix("dask.groupby")
                # Returns {
                #   "dask.groupby.aggregate.split_out": 1,
                #   "dask.groupby.aggregate.split_every": 1
                # }

                sql_config.get_config_by_prefix("dask")
                # Returns {
                #   "dask.groupby.aggregate.split_out": 1,
                #   "dask.groupby.aggregate.split_every": 1,
                #   "dask.sort.persist": True
                #   }

                sql_config.get_config_by_prefix("dask.sort")
                # Returns {"dask.sort.persist": True}

                sql_config.get_config_by_prefix("missing.key")
                sql_config.get_config_by_prefix(None)
                # Both return {}

        """
        return (
            {
                key: val
                for key, val in self.config_dict.items()
                if key.startswith(config_prefix)
            }
            if config_prefix
            else {}
        )
