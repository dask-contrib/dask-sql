import pandas as pd
from packaging.version import parse as parseVersion

_pandas_version = parseVersion(pd.__version__)
FLOAT_NAN_IMPLEMENTED = _pandas_version >= parseVersion("1.2.0")
INT_NAN_IMPLEMENTED = _pandas_version >= parseVersion("1.0.0")
