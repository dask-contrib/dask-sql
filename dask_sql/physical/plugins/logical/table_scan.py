class LogicalTableScanPlugin:
   class_name = "org.apache.calcite.rel.logical.LogicalTableScan"

   def __call__(self, ral, tables):
        table = ral.getTable()

        table_names = [str(n) for n in table.getQualifiedName()]
        assert table_names[0] == "schema"
        assert len(table_names) == 2
        table_name = table_names[1]

        row_type = table.getRowType()
        field_specifications = [str(f) for f in row_type.getFieldNames()]

        return tables[table_name][field_specifications]