from distutils.version import LooseVersion

import pandas as pd

_pandas_version = LooseVersion(pd.__version__)
FLOAT_NAN_IMPLEMENTED = _pandas_version >= LooseVersion("1.2.0")
INT_NAN_IMPLEMENTED = _pandas_version >= LooseVersion("1.0.0")
