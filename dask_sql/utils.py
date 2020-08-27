from collections import defaultdict

import numpy as np


def is_frame(df):
    """
    Check if something is a dataframe (and not a scalar or none)
    """
    return df is not None and not np.isscalar(df)


class Pluggable:
    """
    Helper class for everything which can be extended by plugins.
    Basically just a mapping of a name to the stored plugin
    for ever class.
    Please note that the plugins are stored
    in this single class, which makes simple extensions possible.
    """

    __plugins = defaultdict(dict)

    @classmethod
    def add_plugin(cls, name, plugin, replace=True):
        """Add a plugin with the given name"""
        if not replace and name in Pluggable.__plugins[cls]:
            return

        Pluggable.__plugins[cls][name] = plugin

    @classmethod
    def get_plugin(cls, name):
        """Get a plugin with the given name"""
        return Pluggable.__plugins[cls][name]
