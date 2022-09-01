import logging
import os
import sys
import tempfile
import traceback
from argparse import ArgumentParser
from functools import partial
from typing import Union

import pandas as pd
from dask.datasets import timeseries
from dask.distributed import Client, as_completed
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.history import FileHistory
from prompt_toolkit.shortcuts import ProgressBar
from pygments.lexers.sql import SqlLexer

try:
    # prompt_toolkit version >= 2
    from prompt_toolkit.lexers import PygmentsLexer
except ImportError:  # pragma: no cover
    # prompt_toolkit version < 2
    from prompt_toolkit.layout.lexers import PygmentsLexer

from dask_sql.context import Context

meta_command_completer = WordCompleter(
    ["\\l", "\\d?", "\\dt", "\\df", "\\de", "\\dm", "\\conninfo", "quit"]
)


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
        # make sure everytime dask-sql  uses same history file
        kwargs = {
            "lexer": lexer,
            "history": FileHistory(
                os.path.join(tempfile.gettempdir(), "dask-sql-history")
            ),
            "auto_suggest": AutoSuggestFromHistory(),
            "completer": meta_command_completer,
        }
        try:
            # Version >= 2.0.1: we can use the session object
            from prompt_toolkit import PromptSession

            session = PromptSession(**kwargs)
            self.prompt = session.prompt
        except ImportError:
            # Version < 2.0: there is no session object
            from prompt_toolkit.shortcuts import prompt

            self.prompt = partial(prompt, **kwargs)


def _display_markdown(content, **kwargs):
    df = pd.DataFrame(content, **kwargs)
    print(df.to_markdown(tablefmt="fancy_grid"))


def _parse_meta_command(sql):
    command, _, arg = sql.partition(" ")
    return command, arg.strip()


def _meta_commands(sql: str, context: Context, client: Client) -> Union[bool, Client]:
    """
    parses metacommands and prints their result
    returns True if meta commands detected
    """
    cmd, schema_name = _parse_meta_command(sql)
    available_commands = [
        ["\\l", "List schemas"],
        ["\\d?, help, ?", "Show available commands"],
        ["\\conninfo", "Show Dask cluster info"],
        ["\\dt [schema]", "List tables"],
        ["\\df [schema]", "List functions"],
        ["\\dm [schema]", "List models"],
        ["\\de [schema]", "List experiments"],
        ["\\dss [schema]", "Switch schema"],
        ["\\dsc [dask scheduler address]", "Switch Dask cluster"],
        ["quit", "Quits dask-sql-cli"],
    ]
    if cmd == "\\dsc":
        # Switch Dask cluster
        _, scheduler_address = _parse_meta_command(sql)
        client = Client(scheduler_address)
        return client  # pragma: no cover
    schema_name = schema_name or context.schema_name
    if cmd == "\\d?" or cmd == "help" or cmd == "?":
        _display_markdown(available_commands, columns=["Commands", "Description"])
    elif cmd == "\\l":
        _display_markdown(context.schema.keys(), columns=["Schemas"])
    elif cmd == "\\dt":
        _display_markdown(context.schema[schema_name].tables.keys(), columns=["Tables"])
    elif cmd == "\\df":
        _display_markdown(
            context.schema[schema_name].functions.keys(), columns=["Functions"]
        )
    elif cmd == "\\de":
        _display_markdown(
            context.schema[schema_name].experiments.keys(), columns=["Experiments"]
        )
    elif cmd == "\\dm":
        _display_markdown(context.schema[schema_name].models.keys(), columns=["Models"])
    elif cmd == "\\conninfo":
        cluster_info = [
            ["Dask scheduler", client.scheduler.__dict__["addr"]],
            ["Dask dashboard", client.dashboard_link],
            ["Cluster status", client.status],
            ["Dask workers", len(client.cluster.workers)],
        ]
        _display_markdown(
            cluster_info, columns=["components", "value"]
        )  # pragma: no cover
    elif cmd == "\\dss":
        if schema_name in context.schema:
            context.schema_name = schema_name
        else:
            print(f"Schema {schema_name} not available")
    elif cmd == "quit":
        print("Quitting dask-sql ...")
        client.close()  # for safer side
        sys.exit()
    elif cmd.startswith("\\"):
        print(
            f"The meta command {cmd} not available, please use commands from below list"
        )
        _display_markdown(available_commands, columns=["Commands", "Description"])
    else:
        # nothing detected probably not a meta command
        return False
    return True


def cmd_loop(
    context: Context = None,
    client: Client = None,
    startup=False,
    log_level=None,
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

        meta_command_detected = _meta_commands(text, context=context, client=client)
        if isinstance(meta_command_detected, Client):
            client = meta_command_detected

        if not meta_command_detected:
            try:
                df = context.sql(text, return_futures=True)
                if df is not None:  # some sql commands returns None
                    df = df.persist()
                    # Now turn it into a list of futures
                    futures = client.futures_of(df)
                    with ProgressBar() as pb:
                        for _ in pb(
                            as_completed(futures), total=len(futures), label="Executing"
                        ):
                            continue
                        df = df.compute()
                        print(df.to_markdown(tablefmt="fancy_grid"))

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
