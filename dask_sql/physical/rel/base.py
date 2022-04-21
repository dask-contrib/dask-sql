import logging
from typing import TYPE_CHECKING, List

import dask.dataframe as dd

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.mappings import cast_column_type, sql_to_python_type

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import DaskRelDataType, DaskTable, LogicalPlan

logger = logging.getLogger(__name__)


class BaseRelPlugin:
    """
    Base class for all plugins to convert between
    a RelNode to a python expression (dask dataframe).

    Derived classed needs to override the class_name attribute
    and the convert method.
    """

    class_name = None

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> dd.DataFrame:
        """Base method to implement"""
        raise NotImplementedError

    @staticmethod
    def fix_column_to_row_type(cc: ColumnContainer, column_names) -> ColumnContainer:
        """
        Make sure that the given column container
        has the column names specified by the row type.
        We assume that the column order is already correct
        and will just "blindly" rename the columns.
        """
        # field_names = [str(x) for x in row_type.getFieldNames()]

        logger.debug(f"Renaming {cc.columns} to {column_names}")

        cc = cc.rename(columns=dict(zip(cc.columns, column_names)))

        # TODO: We can also check for the types here and do any conversions if needed
        return cc.limit_to(column_names)

    @staticmethod
    def check_columns_from_row_type(df: dd.DataFrame, row_type: "DaskRelDataType"):
        """
        Similar to `self.fix_column_to_row_type`, but this time
        check for the correct column names instead of
        applying them.
        """
        field_names = [str(x) for x in row_type.getFieldNames()]

        assert list(df.columns) == field_names

        # TODO: similar to self.fix_column_to_row_type, we should check for the types

    @staticmethod
    def assert_inputs(
        rel: "LogicalPlan",
        n: int = 1,
        context: "dask_sql.Context" = None,
    ) -> List[dd.DataFrame]:
        """
        LogicalPlan nodes build on top of others.
        Those are called the "input" of the LogicalPlan.
        This function asserts that the given LogicalPlan has exactly as many
        input tables as expected and returns them already
        converted into a dask dataframe.
        """
        input_rels = rel.get_inputs()

        assert len(input_rels) == n

        # Late import to remove cycling dependency
        from dask_sql.physical.rel.convert import RelConverter

        return [RelConverter.convert(input_rel, context) for input_rel in input_rels]

    @staticmethod
    def fix_dtype_to_row_type(dc: DataContainer, dask_table: "DaskTable"):
        """
        Fix the dtype of the given data container (or: the df within it)
        to the data type given as argument.
        To prevent unneeded conversions, do only convert if really needed,
        e.g. if the two types are "similar" enough, do not convert.
        Similarity involves the same general type (int, float, string etc)
        but not necessary the size (int64 and int32 are compatible)
        or the nullability.
        TODO: we should check the nullability of the SQL type
        """
        df = dc.df

        for col in dask_table.column_types():
            expected_type = sql_to_python_type(col.get_type_as_str())
            df = cast_column_type(df, col.get_column_name(), expected_type)

        return DataContainer(df, dc.column_container)
