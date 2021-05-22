from ._version import get_version
from .cmd import cmd_loop
from .context import Context
from .server.app import run_server

__version__ = get_version()
