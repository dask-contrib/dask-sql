from argparse import ArgumentParser
from uuid import uuid4
import logging

import dask.distributed
from fastapi import FastAPI, Request, HTTPException
import uvicorn

from dask_sql import Context
import dask_sql
from dask_sql.server.responses import DataResults, QueryResults, ErrorResults

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

    Example:
        It is possible to run an SQL server by using the CLI script in ``dask_sql.server.app``
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
    """
    _init_app(app, context=context, client=client)

    uvicorn.run(app, host=host, port=port)


def _init_app(
    app: FastAPI,
    context: dask_sql.Context = None,
    client: dask.distributed.Client = None,
):
    app.c = context or Context()
    app.future_list = {}
    app.client = client or dask.distributed.Client()
