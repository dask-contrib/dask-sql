from argparse import ArgumentParser

import dask
from dask_sql import run_server


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
    parser.add_argument(
        "--scheduler-address",
        default=None,
        help="Connect to this dask scheduler if given",
    )

    args = parser.parse_args()

    client = None
    if args.scheduler_address:
        client = dask.distributed.Client(args.scheduler_address)

    run_server(host=args.host, port=args.port, client=client)
