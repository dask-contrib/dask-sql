# load in shared rust object first to minimize risk of aarch64 glibc TLS allocation bug
# https://bugzilla.redhat.com/show_bug.cgi?id=1722181
import dask_planner.rust

from . import _version, config
from .cmd import cmd_loop
from .context import Context
from .datacontainer import Statistics
from .server.app import run_server

__version__ = _version.get_versions()["version"]

__all__ = [__version__, cmd_loop, Context, run_server, Statistics]
