import os
from typing import List
import warnings
from collections import defaultdict
import re
from datetime import datetime
import logging
import platform

import dask.dataframe as dd
import numpy as np
import pandas as pd

from dask_sql.datacontainer import DataContainer

logger = logging.getLogger(__name__)


def _set_or_check_java_home():
    """
    We have some assumptions on the JAVA_HOME, namely our jvm comes from
    a conda environment. That does not need to be true, but we should at
    least warn the user.
    """
    if "CONDA_PREFIX" not in os.environ:  # pragma: no cover
        # we are not running in a conda env
        return

    correct_java_path = os.path.normpath(os.environ["CONDA_PREFIX"])
    if platform.system() == "Windows":  # pragma: no cover
        correct_java_path = os.path.normpath(os.path.join(correct_java_path, "Library"))

    if "JAVA_HOME" not in os.environ:  # pragma: no cover
        logger.debug("Setting $JAVA_HOME to $CONDA_PREFIX")
        os.environ["JAVA_HOME"] = correct_java_path
    elif (
        os.path.normpath(os.environ["JAVA_HOME"]) != correct_java_path
    ):  # pragma: no cover
        warnings.warn(
            "You are running in a conda environment, but the JAVA_PATH is not using it. "
            f"If this is by mistake, set $JAVA_HOME to {correct_java_path}, instead of {os.environ['JAVA_HOME']}."
        )


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


class ParsingException(Exception):
    """
    Helper class to format validation and parsing SQL
    exception in a nicer way
    """

    JAVA_MSG_REGEX = "^.*?line (?P<from_line>\d+), column (?P<from_col>\d+)"
    JAVA_MSG_REGEX += "(?: to line (?P<to_line>\d+), column (?P<to_col>\d+))?"

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


def get_table_from_compound_identifier(
    context: "dask_sql.Context", components: List[str]
) -> DataContainer:
    """
    Helper function to return the correct table
    from the stored tables in the context
    with the given name (and maybe also schema name)
    """
    # some queries might also include the database
    # as we do not have such a concept, we just get rid of it
    components = components[-2:]
    tableName = components[-1]

    if len(components) == 2:
        if components[0] != context.schema_name:
            raise AttributeError(f"Schema {components[0]} is not defined.")

    try:
        return context.tables[tableName]
    except KeyError:
        raise AttributeError(f"Table {tableName} is not defined.")
