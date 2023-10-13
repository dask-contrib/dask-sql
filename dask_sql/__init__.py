# FIXME: can we modify TLS model of Rust object to avoid aarch64 glibc bug?
# https://github.com/dask-contrib/dask-sql/issues/1169
from . import _datafusion_lib  # isort:skip

from . import config
from .cmd import cmd_loop
from .context import Context
from .datacontainer import Statistics
from .server.app import run_server

__version__ = "2023.10.0"

__all__ = [__version__, cmd_loop, Context, run_server, Statistics]
