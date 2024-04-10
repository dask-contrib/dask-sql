import importlib
import logging
from collections import defaultdict
from datetime import datetime
from typing import Any
from uuid import uuid4

import dask.dataframe as dd
import numpy as np
import pandas as pd

from dask_sql._datafusion_lib import SqlTypeName
from dask_sql.datacontainer import DataContainer
from dask_sql.mappings import sql_to_python_value

logger = logging.getLogger(__name__)


def is_frame(df):
    """
    Check if something is a dataframe (and not a scalar or none)
    """
    return (
        df is not None
        and not np.isscalar(df)
        and not isinstance(df, type(pd.NA))
        and not isinstance(df, datetime)
    )


def is_datetime(obj):
    """
    Check if a scalar or a series is of datetime type
    """
    return pd.api.types.is_datetime64_any_dtype(obj) or isinstance(obj, datetime)


def convert_to_datetime(df):
    """
    Covert a scalar or a series to datetime type
    """
    if is_frame(df):
        df = df.dt
    else:
        df = pd.to_datetime(df)
    return df


def is_cudf_type(obj):
    """
    Check if an object is a cuDF type
    """
    types = [
        str(type(obj)),
        str(getattr(obj, "_partition_type", "")),
        str(getattr(obj, "_meta", "")),
    ]
    return any("cudf" in obj_type for obj_type in types)


class Pluggable:
    """
    Helper class for everything which can be extended by plugins.
    Basically just a mapping of a name to the stored plugin
    for ever class.
    Please note that the plugins are stored
    in this single class, which makes simple extensions possible.
    """

    __plugins = defaultdict(dict)

    @classmethod
    def add_plugin(cls, names, plugin, replace=True):
        """Add a plugin with the given name"""
        if isinstance(names, str):
            names = [names]

        if not replace and all(name in Pluggable.__plugins[cls] for name in names):
            return

        Pluggable.__plugins[cls].update({name: plugin for name in names})

    @classmethod
    def get_plugin(cls, name):
        """Get a plugin with the given name"""
        return Pluggable.__plugins[cls][name]

    @classmethod
    def get_plugins(cls):
        """Return all registered plugins"""
        return list(Pluggable.__plugins[cls].values())


class ParsingException(Exception):
    """
    Helper class to format validation and parsing SQL
    exception in a nicer way
    """

    def __init__(self, sql, validation_exception_string):
        """
        Create a new exception out of the SQL query and the exception text
        raise by calcite.
        """
        super().__init__(validation_exception_string.strip())


class OptimizationException(Exception):
    """
    Helper class for formatting exceptions that occur while trying to
    optimize a logical plan
    """

    def __init__(self, exception_string):
        """
        Create a new exception out of the SQL query and the exception from DataFusion
        """
        super().__init__(exception_string.strip())


class LoggableDataFrame:
    """Small helper class to print resulting dataframes or series in logging messages"""

    def __init__(self, df):
        self.df = df

    def __str__(self):
        df = self.df
        if isinstance(df, pd.Series) or isinstance(df, dd.Series):
            return f"Series: {(df.name, df.dtype)}"
        if isinstance(df, pd.DataFrame) or isinstance(df, dd.DataFrame):
            return f"DataFrame: {[(col, dtype) for col, dtype in zip(df.columns, df.dtypes)]}"

        elif isinstance(df, DataContainer):
            cols = df.column_container.columns
            dtypes = {col: dtype for col, dtype in zip(df.df.columns, df.df.dtypes)}
            mapping = df.column_container.get_backend_by_frontend_index
            dtypes = [dtypes[mapping(index)] for index in range(len(cols))]
            return f"DataFrame: {[(col, dtype) for col, dtype in zip(cols, dtypes)]}"

        return f"Literal: {df}"


def convert_sql_kwargs(
    sql_kwargs: dict[str, str],
) -> dict[str, Any]:
    """
    Convert the Rust Vec of key/value pairs into a Dict containing the keys and values
    """

    def convert_literal(value):
        if value.isCollection():
            operator_mapping = {
                "SqlTypeName.ARRAY": list,
                "SqlTypeName.MAP": lambda x: dict(zip(x[::2], x[1::2])),
                "SqlTypeName.MULTISET": set,
                "SqlTypeName.ROW": tuple,
            }

            operator = operator_mapping[str(value.getSqlType())]
            operands = [convert_literal(o) for o in value.getOperandList()]

            return operator(operands)
        elif value.isKwargs():
            return convert_sql_kwargs(value.getKwargs())
        else:
            literal_type = value.getSqlType()
            literal_value = value.getSqlValue()

            if literal_type == SqlTypeName.VARCHAR:
                return value.getSqlValue()
            elif literal_type == SqlTypeName.BIGINT and "." in literal_value:
                literal_type = SqlTypeName.DOUBLE

            python_value = sql_to_python_value(literal_type, literal_value)
            return python_value

    return {key: convert_literal(value) for key, value in dict(sql_kwargs).items()}


def import_class(name: str) -> type:
    """
    Import a class with the given name by loading the module
    and referencing the class in the module
    """
    module_path, class_name = name.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def new_temporary_column(df: dd.DataFrame) -> str:
    """Return a new column name which is currently not in use"""
    while True:
        col_name = str(uuid4())

        if col_name not in df.columns:
            return col_name
        else:  # pragma: no cover
            continue
