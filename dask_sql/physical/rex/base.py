from typing import Union, Any

import dask.dataframe as dd


class BaseRexPlugin:
    """
    Base class for all plugins to convert between
    a RexNode to a python expression (dask dataframe column or raw value).

    Derived classed needs to override the class_name attribute
    and the convert method.
    """

    class_name = None

    def convert(
        self, rex: "org.apache.calcite.rex.RexNode", df: dd.DataFrame
    ) -> Union[dd.Series, Any]:
        """Base method to implement"""
        raise NotImplementedError  # pragma: no cover
