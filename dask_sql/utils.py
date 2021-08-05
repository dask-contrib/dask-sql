import importlib
import logging
import re
import sys
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Tuple
from unittest.mock import patch
from uuid import uuid4

import cloudpickle
import dask.dataframe as dd
import numpy as np
import pandas as pd

from dask_sql.datacontainer import DataContainer
from dask_sql.java import com, java, org
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
    def add_plugin(cls, name, plugin, replace=True):
        """Add a plugin with the given name"""
        if not replace and name in Pluggable.__plugins[cls]:
            return

        Pluggable.__plugins[cls][name] = plugin

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

    JAVA_MSG_REGEX = r"^.*?line (?P<from_line>\d+), column (?P<from_col>\d+)"
    JAVA_MSG_REGEX += r"(?: to line (?P<to_line>\d+), column (?P<to_col>\d+))?"

    def __init__(self, sql, validation_exception_string):
        """
        Create a new exception out of the SQL query and the exception text
        raise by calcite.
        """
        message, from_line, from_col = self._extract_message(
            sql, validation_exception_string
        )
        self.from_line = from_line
        self.from_col = from_col

        super().__init__(message)

    @staticmethod
    def _line_with_marker(line, marker_from=0, marker_to=None):
        """
        Add ^ markers under the line specified by the parameters.
        """
        if not marker_to:
            marker_to = len(line)

        return [line] + [" " * marker_from + "^" * (marker_to - marker_from + 1)]

    def _extract_message(self, sql, validation_exception_string):
        """
        Produce a human-readable error message
        out of the Java error message by extracting the column
        and line statements and marking the SQL code with ^ below.

        Typical error message look like:
            org.apache.calcite.runtime.CalciteContextException: From line 3, column 12 to line 3, column 16: Column 'test' not found in any table
            Lexical error at line 4, column 15.  Encountered: "\n" (10), after : "`Te"
        """
        message = validation_exception_string.strip()

        match = re.match(self.JAVA_MSG_REGEX, message)
        if not match:
            # Don't understand this message - just return it
            return message, 1, 1

        match = match.groupdict()

        from_line = int(match["from_line"]) - 1
        from_col = int(match["from_col"]) - 1
        to_line = int(match["to_line"]) - 1 if match["to_line"] else None
        to_col = int(match["to_col"]) - 1 if match["to_col"] else None

        # Add line markings below the sql code
        sql = sql.splitlines()

        if from_line == to_line:
            sql = (
                sql[:from_line]
                + self._line_with_marker(sql[from_line], from_col, to_col)
                + sql[from_line + 1 :]
            )
        elif to_line is None:
            sql = (
                sql[:from_line]
                + self._line_with_marker(sql[from_line], from_col, from_col)
                + sql[from_line + 1 :]
            )
        else:
            sql = (
                sql[:from_line]
                + self._line_with_marker(sql[from_line], from_col)
                + sum(
                    [
                        self._line_with_marker(sql[line])
                        for line in range(from_line + 1, to_line)
                    ],
                    [],
                )
                + self._line_with_marker(sql[to_line], 0, to_col)
                + sql[to_line + 1 :]
            )

        message = f"Can not parse the given SQL: {message}\n\n"
        message += "The problem is probably somewhere here:\n"
        message += "\n\t" + "\n\t".join(sql)

        return message, from_line, from_col


class LoggableDataFrame:
    """Small helper class to print resulting dataframes or series in logging messages"""

    def __init__(self, df):
        self.df = df

    def __str__(self):
        df = self.df
        if isinstance(df, pd.Series) or isinstance(df, dd.Series):
            return f"Series: {(df.name, df.dtype)}"

        elif isinstance(df, DataContainer):
            cols = df.column_container.columns
            dtypes = {col: dtype for col, dtype in zip(df.df.columns, df.df.dtypes)}
            mapping = df.column_container.get_backend_by_frontend_index
            dtypes = [dtypes[mapping(index)] for index in range(len(cols))]
            return f"DataFrame: {[(col, dtype) for col, dtype in zip(cols, dtypes)]}"

        return f"Literal: {df}"


def convert_sql_kwargs(
    sql_kwargs: "java.util.HashMap[org.apache.calcite.sql.SqlNode, org.apache.calcite.sql.SqlNode]",
) -> Dict[str, Any]:
    """
    Convert a HapMap (probably coming from a SqlKwargs class instance)
    into its python equivalent. Basically calls convert_sql_kwargs
    for each of the values, except for some special handling for
    nested key-value parameters, ARRAYs etc. and CHARs (which unfortunately have
    an additional "'" around them if used in convert_sql_kwargs directly).
    """

    def convert_literal(value):
        if isinstance(value, org.apache.calcite.sql.SqlBasicCall):
            operator_mapping = {
                "ARRAY": list,
                "MAP": lambda x: dict(zip(x[::2], x[1::2])),
                "MULTISET": set,
            }

            operator = operator_mapping[str(value.getOperator())]
            operands = [convert_literal(o) for o in value.getOperands()]

            return operator(operands)
        elif isinstance(value, com.dask.sql.parser.SqlKwargs):
            return convert_sql_kwargs(value.getMap())
        else:
            literal_type = str(value.getTypeName())

            if literal_type == "CHAR":
                return str(value.getStringValue())
            elif literal_type == "DECIMAL" and value.isInteger():
                literal_type = "BIGINT"

            literal_value = value.getValue()
            python_value = sql_to_python_value(literal_type, literal_value)
            return python_value

    return {str(key): convert_literal(value) for key, value in dict(sql_kwargs).items()}


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


def make_pickable_without_dask_sql(f):
    """
    Helper function turning f into another function which can be deserialized without dask_sql

    When transporting functions from the client to the dask-workers,
    the are normally pickled. During this process, everything which is "importable"
    (e.g. references to library function calls or classes) are just replaced by references,
    to keep the pickled object small. However, in the case of dask-sql we do not assume
    that the workers also have dask-sql installed, so any usage to dask-sql
    can not be replaced by a pure reference, but by the actual content of the function/class
    etc. To reuse as much as possible from the cloudpickle module, we do a very nasty
    trick here: we replace the logic in the cloudpickle module to find out the origin module
    to make it look to cloudpickle as if those modules are not importable.
    """

    class WhichModuleReplacement:
        """Temporary replacement for the _which_module function"""

        def __init__(self, spec):
            """Store the original function"""
            self._old_which_module = spec

        def __call__(self, obj, name):
            """Ask the original _which_module function for the module and return None, if it is the dask_sql one"""
            module_name = self._old_which_module(obj, name)
            module_name_root = module_name.split(".", 1)[0]
            if module_name_root == "dask_sql":
                return None
            return module_name

    with patch(
        "cloudpickle.cloudpickle._whichmodule",
        spec=True,
        new_callable=WhichModuleReplacement,
    ):
        pickled_f = cloudpickle.dumps(f)

    def wrapped_f(*args, **kwargs):
        import cloudpickle

        f = cloudpickle.loads(pickled_f)
        return f(*args, **kwargs)

    return wrapped_f
