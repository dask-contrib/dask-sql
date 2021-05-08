from typing import Any, Optional, Tuple, Union

import dask.dataframe as dd

import dask_sql
from dask_sql.datacontainer import DataContainer
from dask_sql.java import org


class OutputColumn:
    """Return type of all Rex Plugins. Can store a value or a column reference"""

    def get(self, dc: DataContainer) -> Union[Any, dd.Series]:
        """Call this function to turn the reference to a real value (either a scalar or a series)"""
        raise NotImplementedError


class ScalarValue(OutputColumn):
    """Data class to contain a scalar literal value, used as return of a rex conversion"""

    def __init__(self, value: Any) -> None:
        self._value: Any = value

    def get(self, dc: Optional[DataContainer] = None) -> Any:
        return self._value


class ColumnReference(OutputColumn):
    """Data class to contain a column name reference, used as return of a rex conversion"""

    def __init__(self, column_name: str) -> None:
        self._column_name: str = column_name

    def get(self, dc: DataContainer) -> Any:
        df = dc.df
        return df[self._column_name]


class BaseRexPlugin:
    """
    Base class for all plugins to convert between
    a RexNode to a python expression
    (dask dataframe column or raw value).

    The column should be added to the given dataframe and returned as
    a ColumnReference, whereas the literal value should be returned
    as a ScalarValue.
    Derived classed needs to override the class_name attribute
    and the convert method.

    Please also have a look into the RexConverter for more
    documentation on the column outputs.
    """

    class_name = None

    def convert(
        self,
        rex: org.apache.calcite.rex.RexNode,
        dc: DataContainer,
        context: "dask_sql.Context",
    ) -> Tuple[OutputColumn, DataContainer]:
        """Base method to implement"""
        raise NotImplementedError
