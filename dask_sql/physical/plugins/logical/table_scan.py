from dask_sql.physical.ral import fix_column_to_row_type


class LogicalTableScanPlugin:
    class_name = "org.apache.calcite.rel.logical.LogicalTableScan"

    def __call__(self, ral, tables):
        assert len(ral.getInputs()) == 0

        table = ral.getTable()

        table_names = [str(n) for n in table.getQualifiedName()]
        assert table_names[0] == "schema"
        assert len(table_names) == 2
        table_name = table_names[1]

        row_type = table.getRowType()
        field_specifications = [str(f) for f in row_type.getFieldNames()]

        df = tables[table_name][field_specifications]

        df = fix_column_to_row_type(df, ral.getRowType())

        return df
