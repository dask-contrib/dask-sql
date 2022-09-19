import json
from typing import TYPE_CHECKING, Dict, List

from dask_sql.mappings import _SQL_TO_PYTHON_FRAMES
from dask_sql.physical.rex.core import RexCallPlugin

if TYPE_CHECKING:
    import dask_sql

# That is definitely not pretty, but there seems to be no better way...
KEYWORDS = [
    "and",
    "as",
    "asc",
    "between",
    "by",
    "columns",
    "count",
    "create",
    "delete",
    "desc",
    "describe",
    "distinct",
    "exists",
    "from",
    "group",
    "having",
    "if",
    "in",
    "inner",
    "insert",
    "into",
    "is",
    "join",
    "left",
    "like",
    "model",
    "not",
    "on",
    "or",
    "order",
    "outer",
    "right",
    "schemas",
    "select",
    "set",
    "show",
    "table",
    "union",
    "where",
]


def ipython_integration(
    context: "dask_sql.Context", auto_include: bool
) -> None:  # pragma: no cover
    """Integrate the context with jupyter notebooks. Have a look into :ref:`Context.ipython_magic`."""
    _register_ipython_magic(context, auto_include=auto_include)
    _register_syntax_highlighting()


def _register_ipython_magic(
    c: "dask_sql.Context", auto_include: bool
) -> None:  # pragma: no cover
    from IPython.core.magic import register_line_cell_magic

    def sql(line, cell=None):
        if cell is None:
            # the magic function was called inline
            cell = line

        dataframes = {}
        if auto_include:
            dataframes = c._get_tables_from_stack()

        return c.sql(cell, return_futures=False, dataframes=dataframes)

    # Register a new magic function
    magic_func = register_line_cell_magic(sql)
    magic_func.MAGIC_NO_VAR_EXPAND_ATTR = True


def _register_syntax_highlighting():  # pragma: no cover
    from IPython.core import display

    types = map(str, _SQL_TO_PYTHON_FRAMES.keys())
    functions = list(RexCallPlugin.OPERATION_MAPPING.keys())

    # Create a new mimetype
    mime_type = {
        "name": "sql",
        "keywords": _create_set(KEYWORDS + functions),
        "builtin": _create_set(types),
        "atoms": _create_set(["false", "true", "null"]),
        # "operatorChars": /^[*\/+\-%<>!=~&|^]/,
        "dateSQL": _create_set(["time"]),
        # More information
        # https://opensource.apple.com/source/WebInspectorUI/WebInspectorUI-7600.8.3/UserInterface/External/CodeMirror/sql.js.auto.html
        "support": _create_set(["ODBCdotTable", "doubleQuote", "zerolessFloat"]),
    }

    display.display_javascript(raw=True)


def _create_set(keys: List[str]) -> Dict[str, bool]:  # pragma: no cover
    """Small helper function to turn a list into the correct format for codemirror"""
    return {key: True for key in keys}
