"""DB-API implementation backed by HiveServer2 (Thrift API)

See http://www.python.org/dev/peps/pep-0249/

Many docstrings in this file are based on the PEP, which is in the public domain.
"""

from __future__ import absolute_import, unicode_literals

import datetime
import logging
import re
from decimal import Decimal

from pyhive import common
from pyhive.exc import *  # noqa : F403

from dask_sql import Context

# PEP 249 module globals
apilevel = "2.0"
threadsafety = 2  # Threads may share the module and connections.
paramstyle = "pyformat"  # Python extended format codes, e.g. ...WHERE name=%(name)s

_logger = logging.getLogger(__name__)

_TIMESTAMP_PATTERN = re.compile(r"(\d+-\d+-\d+ \d+:\d+:\d+(\.\d{,6})?)")


def _parse_timestamp(value):
    if value:
        match = _TIMESTAMP_PATTERN.match(value)
        if match:
            if match.group(2):
                format = "%Y-%m-%d %H:%M:%S.%f"
                # use the pattern to truncate the value
                value = match.group()
            else:
                format = "%Y-%m-%d %H:%M:%S"
            value = datetime.datetime.strptime(value, format)
        else:
            raise Exception('Cannot convert "{}" into a datetime'.format(value))
    else:
        value = None
    return value


TYPES_CONVERTER = {"DECIMAL_TYPE": Decimal, "TIMESTAMP_TYPE": _parse_timestamp}


class HiveParamEscaper(common.ParamEscaper):
    def escape_string(self, item):
        # backslashes and single quotes need to be escaped
        # TODO verify against parser
        # Need to decode UTF-8 because of old sqlalchemy.
        # Newer SQLAlchemy checks dialect.supports_unicode_binds before encoding Unicode strings
        # as byte strings. The old version always encodes Unicode as byte strings, which breaks
        # string formatting here.
        if isinstance(item, bytes):
            item = item.decode("utf-8")
        return "'{}'".format(
            item.replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace("\r", "\\r")
            .replace("\n", "\\n")
            .replace("\t", "\\t")
        )


_escaper = HiveParamEscaper()


def connect(*args, **kwargs):
    """Constructor for creating a connection to the database. See class :py:class:`Connection` for
    arguments.

    :returns: a :py:class:`Connection` object.
    """
    return Connection(*args, **kwargs)


class Connection(object):
    """Wraps a dask_sql.Context"""

    def __init__(
        self,
        host,
        port,
        database,
        username,
        password,
        auth=None,
        configuration=None,
    ):
        self._context = Context()

    def __enter__(self):
        """Context should already be initialized by __init__"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Call close"""
        self.close()

    def close(self):
        """Close the underlying Context"""
        self._context = None

    def commit(self):
        """Dask-SQL does not support transactions, so this does nothing."""
        pass

    def cursor(self, *args, **kwargs):
        """Return a new :py:class:`Cursor` object using the connection."""
        return Cursor(self, *args, **kwargs)

    def rollback(self):
        raise NotSupportedError(  # noqa : F405
            "Dask-SQL does not have transactions"
        )  # pragma: no cover


class Cursor(object):
    """These objects represent a database cursor, which is used to manage the context of a fetch
    operation.

    Cursors are not isolated, i.e., any changes done to the database by a cursor are immediately
    visible by other cursors or connections.
    """

    def __init__(self, connection, arraysize=1000):
        self._connection = connection
        self._arraysize = arraysize
        self._last_result = None
        self._description = None

    @property
    def rowcount(self):
        """By default, return -1 to indicate that this is not supported."""
        return -1

    @property
    def arraysize(self):
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value):
        """Array size cannot be None, and should be an integer"""
        default_arraysize = 1000
        try:
            self._arraysize = int(value) or default_arraysize
        except TypeError:
            self._arraysize = default_arraysize

    @property
    def description(self):
        """This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences contains information describing one result column:

        - name
        - type_code
        - display_size (None in current implementation)
        - internal_size (None in current implementation)
        - precision (None in current implementation)
        - scale (None in current implementation)
        - null_ok (always True in current implementation)

        This attribute will be ``None`` for operations that do not return rows or if the cursor has
        not had an operation invoked via the :py:meth:`execute` method yet.

        The ``type_code`` can be interpreted by comparing it to the Type Objects specified in the
        section below.
        """
        if self._last_result is None:
            return None
        if self._description is None:
            self._description = []
            for col in self._last_result.columns:
                self._description.append(
                    (
                        col,
                        str(self._last_result[col].dtype),
                        None,
                        None,
                        None,
                        None,
                        True,
                    )
                )
        return self._description

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Close the operation handle"""
        return None

    def execute(self, operation, parameters=None, **kwargs):
        """Prepare and execute a database operation (query or command).

        Return values are not defined.
        """

        # Prepare statement
        if parameters is None:
            sql = operation
        else:
            sql = operation % _escaper.escape_args(parameters)

        self._last_result = self._connection._context.sql(sql, **kwargs)
        self._row_number = None if self._last_result is None else 0

    def executemany(self, operation, seq_of_parameters, **kwargs):
        """Prepare a database operation (query or command) and then execute it against all parameter
        sequences or mappings found in the sequence ``seq_of_parameters``.

        Only the final result set is retained.

        Return values are not defined.
        """
        for parameters in seq_of_parameters[:-1]:
            self.execute(operation, parameters, kwargs)
        if seq_of_parameters:
            self.execute(operation, seq_of_parameters[-1], kwargs)

    def fetchone(self):
        """Fetch the next row of a query result set, returning a single sequence, or ``None`` when
        no more data is available.

        An ``IndexError`` is raised if the previous call to :py:meth:`execute` did not produce any
        result set or no call was issued yet.
        """
        if self._last_result is None:
            raise IndexError("No result set")
        else:
            res = self._last_result.compute().iloc[self._row_number].values.tolist()
            self._row_number += 1
            return res

    def fetchmany(self, size=None):
        """Fetch the next set of rows of a query result, returning a sequence of sequences (e.g.
        a list of tuples). An empty sequence is returned when no more rows are available.

        The number of rows to fetch per call is specified by the parameter. If it is not given, the
        cursor's :py:attr:`arraysize` determines the number of rows to be fetched. The method should
        try to fetch as many rows as indicated by the size parameter. If this is not possible due to
        the specified number of rows not being available, fewer rows may be returned.

        An ``IndexError`` is raised if the previous call to :py:meth:`execute` did not produce any
        result set or no call was issued yet.
        """
        if self._last_result is None:
            raise IndexError("No result set")
        else:
            size = size or self.arraysize
            res = (
                self._last_result.compute()
                .iloc[self._row_number : size]
                .values.tolist()
            )
            self._row_number += size
            return res

    def fetchall(self):
        """Fetch all (remaining) rows of a query result, returning them as a sequence of sequences
        (e.g. a list of tuples).

        An ``IndexError`` is raised if the previous call to :py:meth:`execute` did not produce any
        result set or no call was issued yet.
        """
        if self._last_result is None:
            raise IndexError("No result set")
        else:
            res = self._last_result.iloc[self._row_number :].compute().values.tolist()
            self._row_number = len(res)
            return res
