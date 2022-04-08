from . import config
from ._version import get_versions
from .cmd import cmd_loop
from .context import Context
from .datacontainer import Statistics

# from .server.app import run_server

__version__ = get_versions()["version"]
del get_versions

# TODO: re-enable server once CVEs are resolved
# __all__ = [__version__, cmd_loop, Context, run_server, Statistics]
__all__ = [__version__, cmd_loop, Context, Statistics]
