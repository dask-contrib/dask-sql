from typing import Dict

import dask.dataframe as dd

from dask_sql.physical.rel.base import BaseRelPlugin


class LogicalTableScanPlugin(BaseRelPlugin):
    """
    A LogicalTableScal is the main ingredient: it will get the data
    from the database. It is always used, when the SQL looks like

        SELECT .... FROM table ....

    We need to get the dask dataframe from the registered
    tables and return the requested columns from it.
    Calcite will always refer to columns via index.
    """

    class_name = "org.apache.calcite.rel.logical.LogicalTableScan"

    def convert(
        self, rel: "org.apache.calcite.rel.RelNode", tables: Dict[str, dd.DataFrame]
    ) -> dd.DataFrame:
        # There should not be any input. This is the first step.
        self.assert_inputs(rel, 0)

        # The table(s) we need to return
        table = rel.getTable()

        # The table names are all names split by "."
        # We assume to always have the form something.something
        # And the first something is fixed to "schema" by the context
        # For us, it makes no difference anyways.
        table_names = [str(n) for n in table.getQualifiedName()]
        assert table_names[0] == "schema"
        assert len(table_names) == 2
        table_name = table_names[1]

        df = tables[table_name]

        # Make sure we only return the requested columns
        row_type = table.getRowType()
        field_specifications = [str(f) for f in row_type.getFieldNames()]
        df = df[field_specifications]

        df = self.fix_column_to_row_type(df, rel.getRowType())
        return df
