import logging
import sys
import traceback
from argparse import ArgumentParser
from functools import partial

import pandas as pd
from dask.datasets import timeseries
from dask.distributed import Client
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
    ["\l", "\d?", "\dt", "\df", "\de", "\dm", "\conninfo", "quit"]
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
        kwargs = {
            "lexer": lexer,
            "history": FileHistory("dask-sql-history"),
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


def display_markdown(content, **kwargs):
    df = pd.DataFrame(content, **kwargs)
    print(df.to_markdown(tablefmt="fancy_grid"))
    return True


def parse_meta_command(sql):
    command, _, arg = sql.partition(" ")
    command = command.strip().replace("+", "")
    return command, arg.strip()


def meta_commands(sql: str, context: Context, client: Client) -> bool:
    """
     parses metacommands and prints their result
     returns True if meta commands detected
    """
    cmd, schema_name = parse_meta_command(sql)
    if schema_name == "":
        schema_name = context.schema_name
    if cmd == "\d?":
        available_commands = [
            ["\l", "List Schemas"],
            ["\d?]", "Show Commands"],
            ["\conninfo", "Show Dask Cluster info"],
            ["\dt [schema]", "List tables"],
            ["\df [schema]", "List functions"],
            ["\dm [schema]", "List models"],
            ["\de [schema]", "List experiments"],
            ["quit", "Quits dask-sql-cli"],
        ]
        return display_markdown(available_commands, columns=["Commands", "Describtion"])
    elif cmd == "\l":
        return display_markdown(context.schema.keys())
    elif cmd == "\dt":
        return display_markdown(context.schema[schema_name].tables.keys())
    elif cmd == "\df":
        return display_markdown(context.schema[schema_name].functions.keys())
    elif cmd == "\de":
        return display_markdown(context.schema[schema_name].experiments.keys())
    elif cmd == "\dm":
        return display_markdown(context.schema[schema_name].models.keys())
    elif cmd == "\conninfo":
        print("Dask cluster info")
        cluster_info = [
            ["Dask scheduler", client.scheduler.__dict__["addr"]],
            ["Dask Dashboard", client.dashboard_link],
            ["Cluster status", client.status],
            ["Dask Workers", len(client.cluster.workers)],
        ]
        return display_markdown(cluster_info, columns=["components", "value"])
    elif cmd == "quit":
        print("Quiting dask-sql ...")
        sys.exit()
    return False


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
            sys.exit()
        except EOFError:
            break

        text = text.rstrip(";").strip()

        if not text:
            continue

        meta_command_detected = meta_commands(text, context=context, client=client)

        def sqlCaller(text):
            """
            This function is for showing progress bar
            """
            yield context.sql(text, return_futures=False)

        if not meta_command_detected:
            try:
                with ProgressBar() as pb:
                    for df in pb(sqlCaller(text), total=1, label="Executing"):
                        if df is not None:  # some sql commands returns None
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
