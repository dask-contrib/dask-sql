import dask
import pandas as pd
import prompt_toolkit
from packaging.version import parse as parseVersion

try:
    import pyarrow as pa
except ImportError:
    pa = None

_pandas_version = parseVersion(pd.__version__)
_pyarrow_version = parseVersion(pa.__version__) if pa else parseVersion("0.0.0")
_prompt_toolkit_version = parseVersion(prompt_toolkit.__version__)
_dask_version = parseVersion(dask.__version__)

INDEXER_WINDOW_STEP_IMPLEMENTED = _pandas_version >= parseVersion("1.5.0")
PANDAS_GT_200 = _pandas_version >= parseVersion("2.0.0")

# TODO: remove if prompt-toolkit min version gets bumped
PIPE_INPUT_CONTEXT_MANAGER = _prompt_toolkit_version >= parseVersion("3.0.29")

# TODO: remove when dask min version gets bumped
BROADCAST_JOIN_SUPPORT_WORKING = _dask_version > parseVersion("2023.1.0")

# Parquet predicate-support version checks
PQ_NOT_IN_SUPPORT = parseVersion(dask.__version__) > parseVersion("2023.5.1")
PQ_IS_SUPPORT = parseVersion(dask.__version__) >= parseVersion("2023.3.1")

DASK_CUDF_TODATETIME_SUPPORT = _dask_version >= parseVersion("2023.5.1")

PA_GT_700 = _pyarrow_version >= parseVersion("7.0.0")
