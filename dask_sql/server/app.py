from argparse import ArgumentParser

from dask_sql.server.responses import DataResults, QueryResults, ErrorResults
from fastapi import FastAPI, Request
import uvicorn

from dask_sql import Context

app = FastAPI()


@app.get("/v1/empty")
async def empty(request: Request):
    """
    Helper endpoint returning an empty
    result.
    """
    return QueryResults(request=request)


@app.post("/v1/statement")
async def query(request: Request):
    """
    Main endpoint returning query results
    in the presto on wire format.
    """
    try:
        sql = (await request.body()).decode().strip()
        df = request.app.c.sql(sql)

        return DataResults(df, request=request)
    except Exception as e:
        return ErrorResults(e, request=request)


def run_server(
    context: Context = None, host: str = "0.0.0.0", port: int = 8080
):  # pragma: no cover
    """
    Run a HTTP server for answering SQL queries using ``dask-sql``.
    It uses the `Presto Wire Protocol <https://github.com/prestodb/presto/wiki/HTTP-Protocol>`_
    for communication.
    This means, it has a single POST endpoint `v1/statement`, which answers
    SQL queries (as string in the body) with the output as a JSON
    (in the format described in the documentation above).
    Every SQL expression that ``dask-sql`` understands can be used here.

    Note:
        The presto protocol also includes some statistics on the query
        in the response.
        These statistics are currently only filled with placeholder variables.

    Args:
        context (:obj:`dask_sql.Context`): If set, use this context instead of an empty one.
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
    if context is None:
        context = Context()

    app.c = context

    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="The host interface to listen on (defaults to all interfaces)",
    )
    parser.add_argument(
        "--port", default=8080, help="The port to listen on (defaults to 8080)"
    )

    args = parser.parse_args()

    run_server(host=args.host, port=args.port)
