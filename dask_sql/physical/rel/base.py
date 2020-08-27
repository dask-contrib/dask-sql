from typing import Dict, List

import dask.dataframe as dd


class BaseRelPlugin:
    """
    Base class for all plugins to convert between
    a RelNode to a python expression (dask dataframe).

    Derived classed needs to override the class_name attribute
    and the convert method.
    """

    class_name = None

    def convert(
        self, rel: "org.apache.calcite.rel.RelNode", tables: Dict[str, dd.DataFrame]
    ) -> dd.DataFrame:
        """Base method to implement"""
        raise NotImplementedError  # pragma: no cover

    @staticmethod
    def fix_column_to_row_type(
        df: dd.DataFrame, row_type: "org.apache.calcite.rel.type.RelDataType"
    ) -> dd.DataFrame:
        """
        Make sure that the given dask dataframe
        has the column names specified by the row type.
        We assume that the column order is already correct
        and will just "blindly" rename the columns.
        """
        field_names = [str(x) for x in row_type.getFieldNames()]

        df = df.rename(columns=dict(zip(df.columns, field_names)))

        # TODO: We can also check for the types here and do any conversions if needed
        return df[field_names]

    @staticmethod
    def check_columns_from_row_type(
        df: dd.DataFrame, row_type: "org.apache.calcite.rel.type.RelDataType"
    ):
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
        rel: "org.apache.calcite.rel.RelNode",
        n: int = 1,
        tables: Dict[str, dd.DataFrame] = None,
    ) -> List[dd.DataFrame]:
        """
        Many RelNodes build on top of others.
        Those are the "input" of these RelNodes.
        This function asserts that the given RelNode has exactly as many
        input tables as expected and returns them already
        converted into a dask dataframe.
        """
        input_rels = rel.getInputs()

        assert len(input_rels) == n

        # Late import to remove cycling dependency
        from dask_sql.physical.rel.convert import RelConverter

        return [RelConverter.convert(input_rel, tables) for input_rel in input_rels]

    @staticmethod
    def make_unique(df, prefix="col"):
        """
        Make sure we have unique column names by calling each column

            prefix_number

        where number is the column index.
        """
        return df.rename(
            columns={col: f"{prefix}_{i}" for i, col in enumerate(df.columns)}
        )
