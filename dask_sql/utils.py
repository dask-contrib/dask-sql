from collections import defaultdict
import re

import numpy as np
import pandas as pd


def is_frame(df):
    """
    Check if something is a dataframe (and not a scalar or none)
    """
    return df is not None and not np.isscalar(df) and not isinstance(df, type(pd.NA))


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
        message = self._extract_message(sql, validation_exception_string)
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
            return message

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

        return message
