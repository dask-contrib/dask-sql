import logging
from typing import TYPE_CHECKING, Any, Union

import dask.dataframe as dd

import dask_sql
from dask_sql.datacontainer import DataContainer

if TYPE_CHECKING:
    from dask_planner.rust import Expression, LogicalPlan

logger = logging.getLogger(__name__)


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
        rel: "LogicalPlan",
        rex: "Expression",
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> Union[dd.Series, Any]:
        """Base method to implement"""
        raise NotImplementedError
