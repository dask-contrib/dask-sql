import time
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
    context: "dask_sql.Context",
    auto_include: bool,
    disable_highlighting: bool,
) -> None:  # pragma: no cover
    """Integrate the context with jupyter notebooks. Have a look into :ref:`Context.ipython_magic`."""
    _register_ipython_magic(context, auto_include=auto_include)
    if not disable_highlighting:
        _register_syntax_highlighting()


def _register_ipython_magic(
    c: "dask_sql.Context", auto_include: bool
) -> None:  # pragma: no cover
    from IPython.core.magic import needs_local_scope, register_line_cell_magic

    @needs_local_scope
    def sql(line, cell, local_ns):
        if cell is None:
            # the magic function was called inline
            cell = line

        sql_statement = cell.format(**local_ns)

        dataframes = {}
        if auto_include:
            dataframes = c._get_tables_from_stack()

        t0 = time.time()
        res = c.sql(sql_statement, return_futures=False, dataframes=dataframes)
        if (
            "CREATE OR REPLACE TABLE" in sql_statement
            or "CREATE OR REPLACE VIEW" in sql_statement
        ):
            table = sql_statement.split("CREATE OR REPLACE")[1]
            table = table.replace("TABLE", "").replace("VIEW", "").split()[0].strip()
            res = c.sql(f"SELECT * FROM {table}").tail()
        elif "CREATE TABLE" in sql_statement or "CREATE VIEW" in sql_statement:
            table = sql_statement.split("CREATE")[1]
            table = table.replace("TABLE", "").replace("VIEW", "").split()[0].strip()
            res = c.sql(f"SELECT * FROM {table}").tail()
        print(f"Execution time: {time.time() - t0:.2f}s")
        return res

    # Register a new magic function
    magic_func = register_line_cell_magic(sql)
    magic_func.MAGIC_NO_VAR_EXPAND_ATTR = True


def _register_syntax_highlighting():  # pragma: no cover
    import json

    from IPython.core import display

    # JS snippet to use the created mime type highlighthing
    _JS_ENABLE_DASK_SQL = r"""
    require(['notebook/js/codecell'], function(codecell) {
        codecell.CodeCell.options_default.highlight_modes['magic_text/x-dasksql'] = {'reg':[/%%sql/]} ;
        Jupyter.notebook.events.on('kernel_ready.Kernel', function(){
        Jupyter.notebook.get_cells().map(function(cell){
            if (cell.cell_type == 'code'){ cell.auto_highlight(); } }) ;
        });
    });
    """

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

    # Code original from fugue-sql, adjusted for dask-sql and using some more customizations
    js = (
        r"""
    require(["codemirror/lib/codemirror"]);

    // We define a new mime type for syntax highlighting
    CodeMirror.defineMIME("text/x-dasksql", """
        + json.dumps(mime_type)
        + r"""
    );
    CodeMirror.modeInfo.push({
        name: "Dask SQL",
        mime: "text/x-dasksql",
        mode: "sql"
    });
    """
    )

    display.display_javascript(js + _JS_ENABLE_DASK_SQL, raw=True)


def _create_set(keys: List[str]) -> Dict[str, bool]:  # pragma: no cover
    """Small helper function to turn a list into the correct format for codemirror"""
    return {key: True for key in keys}
