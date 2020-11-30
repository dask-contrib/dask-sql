from typing import Union, Any

import dask.dataframe as dd

from dask_sql.datacontainer import DataContainer


class BaseRexPlugin:
    """
    Base class for all plugins to convert between
    a RexNode to a python expression (dask dataframe column or raw value).

    Derived classed needs to override the class_name attribute
    and the convert method.
    """

    class_name = None

    def convert(
        self,
        rex: "org.apache.calcite.rex.RexNode",
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> Union[dd.Series, Any]:
        """Base method to implement"""
        raise NotImplementedError
