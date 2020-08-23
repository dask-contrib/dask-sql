import numpy as np


def is_frame(df):
    return df is not None and not np.isscalar(df)
