# FIXME: can we modify TLS model of Rust object to avoid aarch64 glibc bug?
# https://github.com/dask-contrib/dask-sql/issues/1169
import dask_planner.rust

from . import _version, config
from .cmd import cmd_loop
from .context import Context
from .datacontainer import Statistics
from .server.app import run_server

__version__ = _version.get_versions()["version"]

__all__ = [__version__, cmd_loop, Context, run_server, Statistics]
