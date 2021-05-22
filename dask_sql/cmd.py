import logging
import traceback
from argparse import ArgumentParser
from functools import partial

import pandas as pd
from dask.datasets import timeseries
from dask.distributed import Client
from pygments.lexers.sql import SqlLexer

try:
    # prompt_toolkit version >= 2
    from prompt_toolkit.lexers import PygmentsLexer
except ImportError:  # pragma: no cover
    # prompt_toolkit version < 2
    from prompt_toolkit.layout.lexers import PygmentsLexer

from dask_sql.context import Context


class CompatiblePromptSession:
    """
    Session object wrapper for the prompt_toolkit module

    In the version jump from 1 to 2, the prompt_toolkit
    introduced a PromptSession object.
    Some environments however (e.g. google collab)
    still rely on an older prompt_toolkit version,
    so we try to support both versions
    with this wrapper object.
    All it does is export a `prompt` function.
    """

    def __init__(self, lexer) -> None:  # pragma: no cover
        try:
            # Version >= 2.0.1: we can use the session object
            from prompt_toolkit import PromptSession

            session = PromptSession(lexer=lexer)
            self.prompt = session.prompt
        except ImportError:
            # Version < 2.0: there is no session object
            from prompt_toolkit.shortcuts import prompt

            self.prompt = partial(prompt, lexer=lexer)


def cmd_loop(
    context: Context = None, client: Client = None, startup=False, log_level=None,
):  # pragma: no cover
    """
    Run a REPL for answering SQL queries using ``dask-sql``.
    Every SQL expression that ``dask-sql`` understands can be used here.

    Args:
        context (:obj:`dask_sql.Context`): If set, use this context instead of an empty one.
        client (:obj:`dask.distributed.Client`): If set, use this dask client instead of a new one.
        startup (:obj:`bool`): Whether to wait until Apache Calcite was loaded
        log_level: (:obj:`str`): The log level of the server and dask-sql

    Example:
        It is possible to run a REPL by using the CLI script in ``dask-sql``
        or by calling this function directly in your user code:

        .. code-block:: python

            from dask_sql import cmd_loop

            # Create your pre-filled context
            c = Context()
            ...

            cmd_loop(context=c)

        Of course, it is also possible to call the usual ``CREATE TABLE``
        commands.
    """
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_colwidth", None)

    logging.basicConfig(level=log_level)

    client = client or Client()
    context = context or Context()

    if startup:
        context.sql("SELECT 1 + 1").compute()

    session = CompatiblePromptSession(lexer=PygmentsLexer(SqlLexer))

    while True:
        try:
            text = session.prompt("(dask-sql) > ")
        except KeyboardInterrupt:
            continue
        except EOFError:
            break

        text = text.rstrip(";").strip()

        if not text:
            continue

        try:
            df = context.sql(text, return_futures=False)
            print(df)
        except Exception:
            traceback.print_exc()


def main():  # pragma: no cover
    parser = ArgumentParser()
    parser.add_argument(
        "--scheduler-address",
        default=None,
        help="Connect to this dask scheduler if given",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        help="Set the log level of the server. Defaults to info.",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    parser.add_argument(
        "--load-test-data",
        default=False,
        action="store_true",
        help="Preload some test data.",
    )
    parser.add_argument(
        "--startup",
        default=False,
        action="store_true",
        help="Wait until Apache Calcite was properly loaded",
    )

    args = parser.parse_args()

    client = None
    if args.scheduler_address:
        client = Client(args.scheduler_address)

    context = Context()
    if args.load_test_data:
        df = timeseries(freq="1d").reset_index(drop=False)
        context.create_table("timeseries", df.persist())

    cmd_loop(
        context=context, client=client, startup=args.startup, log_level=args.log_level
    )


if __name__ == "__main__":
    main()
