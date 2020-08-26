import numpy as np


def is_frame(df):
    """
    Check if something is a dataframe (and not a scalar or none)
    """
    return df is not None and not np.isscalar(df)
