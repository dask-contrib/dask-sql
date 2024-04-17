# FIXME: can we modify TLS model of Rust object to avoid aarch64 glibc bug?
# https://github.com/dask-contrib/dask-sql/issues/1169
from . import _datafusion_lib  # isort:skip

import importlib.metadata

from dask.config import set

from . import config
from .cmd import cmd_loop
from .context import Context
from .datacontainer import Statistics
from .server.app import run_server

# TODO: get pyarrow strings and p2p shuffle working
set(dataframe__convert_string=False, dataframe__shuffle__method="tasks")

__version__ = importlib.metadata.version(__name__)

__all__ = [__version__, cmd_loop, Context, run_server, Statistics]
