import jpype

jpype.addClassPath(
    "./planner/application/target/DaskSQL.jar"
)

jpype.startJVM("-ea", convertStrings=False)

ArrayClass = jpype.JClass("java.util.ArrayList")
ColumnTypeClass = jpype.JClass(
    "com.dask.sql.catalog.domain.CatalogColumnDataType"
)
ColumnClass = jpype.JClass("com.dask.sql.catalog.domain.CatalogColumn")
TableClass = jpype.JClass("com.dask.sql.schema.DaskTable")
DaskSchemaClass = jpype.JClass("com.dask.sql.schema.DaskSchema")
RelationalAlgebraGeneratorClass = jpype.JClass(
    "com.dask.sql.application.RelationalAlgebraGenerator"
)


schema = DaskSchemaClass("main")

tableName = "my_table"
arr = ArrayClass()
for order, column in enumerate(["a", "b"]):
    type_id = 1 #table.column_types[order]
    dataType = ColumnTypeClass.fromTypeId(type_id)
    column = ColumnClass(column, dataType, order)
    arr.add(column)

tableJava = TableClass(tableName, arr)

schema.addTable(tableJava)
generator = RelationalAlgebraGeneratorClass(schema)

print(generator.getRelationalAlgebraString("SELECT SUM(a) FROM my_table GROUP BY b"))
