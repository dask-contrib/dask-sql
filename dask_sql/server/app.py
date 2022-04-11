import asyncio
import logging
from argparse import ArgumentParser
from uuid import uuid4

import dask.distributed
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from nest_asyncio import apply
from uvicorn import Config, Server

from dask_sql.context import Context
from dask_sql.server.presto_jdbc import create_meta_data
from dask_sql.server.responses import DataResults, ErrorResults, QueryResults

app = FastAPI()
logger = logging.getLogger(__name__)


@app.get("/v1/empty")
async def empty(request: Request):
    """
    Helper endpoint returning an empty
    result.
    """
    return QueryResults(request=request)


@app.delete("/v1/cancel/{uuid}")
async def cancel(uuid: str, request: Request):
    """
    Cancel an already running computation
    """
    logger.debug(f"Canceling the request with uuid {uuid}")
    try:
        future = request.app.future_list[uuid]
    except KeyError:
        raise HTTPException(status_code=404, detail="uuid not found")
    future.cancel()
    del request.app.future_list[uuid]

    return {"status": "ok"}


@app.get("/v1/status/{uuid}")
async def status(uuid: str, request: Request):
    """
    Return the status (or the result) of an already running calculation
    """
    logger.debug(f"Accessing the request with uuid {uuid}")
    try:
        future = request.app.future_list[uuid]
    except KeyError:
        raise HTTPException(status_code=404, detail="uuid not found")

    if future.done():
        logger.debug(f"{uuid} is already finished, returning data")
        df = future.result()

        del request.app.future_list[uuid]

        return DataResults(df, request=request)

    logger.debug(f"{uuid} is not already finished")

    status_url = str(request.url)
    return QueryResults(request=request, next_url=status_url)


@app.post("/v1/statement")
async def query(request: Request):
    """
    Main endpoint returning query results
    in the presto on wire format.
    """
    try:
        sql = (await request.body()).decode().strip()
        # required for PrestoDB JDBC driver compatibility
        # replaces queries to unsupported `system` catalog with queries to `system_jdbc`
        # schema created by `create_meta_data(context)` when `jdbc_metadata=True`
        # TODO: explore Trino which should make JDBC compatibility easier but requires
        # changing response headers (see https://github.com/dask-contrib/dask-sql/pull/351)
        sql = sql.replace("system.jdbc", "system_jdbc")
        df = request.app.c.sql(sql)

        if df is None:
            return DataResults(df, request)

        uuid = str(uuid4())
        request.app.future_list[uuid] = request.app.client.compute(df)
        logger.debug(f"Registering {sql} with uuid {uuid}.")

        status_url = str(
            request.url.replace(path=request.app.url_path_for("status", uuid=uuid))
        )
        cancel_url = str(
            request.url.replace(path=request.app.url_path_for("cancel", uuid=uuid))
        )
        return QueryResults(request=request, next_url=status_url, cancel_url=cancel_url)
    except Exception as e:
        return ErrorResults(e, request=request)


def run_server(
    context: Context = None,
    client: dask.distributed.Client = None,
    host: str = "0.0.0.0",
    port: int = 8080,
    startup=False,
    log_level=None,
    blocking: bool = True,
    jdbc_metadata: bool = False,
):  # pragma: no cover
    """
    Run a HTTP server for answering SQL queries using ``dask-sql``.
    It uses the `Presto Wire Protocol <https://github.com/prestodb/presto/wiki/HTTP-Protocol>`_
    for communication.
    This means, it has a single POST endpoint `/v1/statement`, which answers
    SQL queries (as string in the body) with the output as a JSON
    (in the format described in the documentation above).
    Every SQL expression that ``dask-sql`` understands can be used here.

    See :ref:`server` for more information.

    Note:
        The presto protocol also includes some statistics on the query
        in the response.
        These statistics are currently only filled with placeholder variables.

    Args:
        context (:obj:`dask_sql.Context`): If set, use this context instead of an empty one.
        client (:obj:`dask.distributed.Client`): If set, use this dask client instead of a new one.
        host (:obj:`str`): The host interface to listen on (defaults to all interfaces)
        port (:obj:`int`): The port to listen on (defaults to 8080)
        startup (:obj:`bool`): Whether to wait until Apache Calcite was loaded
        log_level: (:obj:`str`): The log level of the server and dask-sql
        blocking: (:obj:`bool`): If running in an environment with an event loop (e.g. a jupyter notebook),
                do not block. The server can be stopped with `context.stop_server()` afterwards.
        jdbc_metadata: (:obj:`bool`): If enabled create JDBC metadata tables using schemas and tables in
                the current dask_sql context

    Example:
        It is possible to run an SQL server by using the CLI script ``dask-sql-server``
        or by calling this function directly in your user code:

        .. code-block:: python

            from dask_sql import run_server

            # Create your pre-filled context
            c = Context()
            ...

            run_server(context=c)

        After starting the server, it is possible to send queries to it, e.g. with the
        `presto CLI <https://prestosql.io/docs/current/installation/cli.html>`_
        or via sqlalchemy (e.g. using the `PyHive <https://github.com/dropbox/PyHive#sqlalchemy>`_ package):

        .. code-block:: python

            from sqlalchemy.engine import create_engine
            engine = create_engine('presto://localhost:8080/')

            import pandas as pd
            pd.read_sql_query("SELECT 1 + 1", con=engine)

        Of course, it is also possible to call the usual ``CREATE TABLE``
        commands.

        If in a jupyter notebook, you should run the following code

        .. code-block:: python

            from dask_sql import Context

            c = Context()
            c.run_server(blocking=False)

            ...

            c.stop_server()

        Note:
            When running in a jupyter notebook without blocking,
            it is not possible to access the SQL server from within the
            notebook, e.g. using sqlalchemy.
            Doing so will deadlock infinitely.

    """
    _init_app(app, context=context, client=client)
    if jdbc_metadata:
        create_meta_data(context)

    if startup:
        app.c.sql("SELECT 1 + 1").compute()

    config = Config(app, host=host, port=port, log_level=log_level)
    server = Server(config=config)

    loop = asyncio.get_event_loop()
    if blocking:
        if loop and loop.is_running():
            apply(loop=loop)

        server.run()
    else:
        if not loop or not loop.is_running():
            raise AttributeError(
                "blocking=True needs a running event loop (e.g. in a jupyter notebook)"
            )
        loop.create_task(server.serve())
        context.sql_server = server


def main():  # pragma: no cover
    """
    CLI version of the :func:`run_server` function.
    """
    parser = ArgumentParser()
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="The host interface to listen on (defaults to all interfaces)",
    )
    parser.add_argument(
        "--port", default=8080, help="The port to listen on (defaults to 8080)"
    )
    parser.add_argument(
        "--scheduler-address",
        default=None,
        help="Connect to this dask scheduler if given",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        help="Set the log level of the server. Defaults to info.",
        choices=uvicorn.config.LOG_LEVELS,
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
        client = dask.distributed.Client(args.scheduler_address)

    context = Context()
    if args.load_test_data:
        df = dask.datasets.timeseries(freq="1d").reset_index(drop=False)
        context.create_table("timeseries", df.persist())

    run_server(
        context=context,
        client=client,
        host=args.host,
        port=args.port,
        startup=args.startup,
        log_level=args.log_level,
    )


def _init_app(
    app: FastAPI,
    context: Context = None,
    client: dask.distributed.Client = None,
):
    app.c = context or Context()
    app.future_list = {}

    try:
        client = client or dask.distributed.Client.current()
    except ValueError:
        client = dask.distributed.Client()
    app.client = client
