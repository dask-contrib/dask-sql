import dask
import pandas as pd
import prompt_toolkit
from packaging.version import parse as parseVersion

_pandas_version = parseVersion(pd.__version__)
_prompt_toolkit_version = parseVersion(prompt_toolkit.__version__)
_dask_version = parseVersion(dask.__version__)

FLOAT_NAN_IMPLEMENTED = _pandas_version >= parseVersion("1.2.0")
INT_NAN_IMPLEMENTED = _pandas_version >= parseVersion("1.0.0")
INDEXER_WINDOW_STEP_IMPLEMENTED = _pandas_version >= parseVersion("1.5.0")

# TODO: remove if prompt-toolkit min version gets bumped
PIPE_INPUT_CONTEXT_MANAGER = _prompt_toolkit_version >= parseVersion("3.0.29")

# TODO: remove when dask min version gets bumped
BROADCAST_JOIN_SUPPORT_WORKING = _dask_version > parseVersion("2023.1.0")
