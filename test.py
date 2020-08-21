import jpype

jpype.addClassPath("./planner/application/target/DaskSQL.jar")
jpype.startJVM("-ea", convertStrings=False)

ColumnTypeClass = jpype.JClass("org.apache.calcite.sql.type.SqlTypeName")
TableClass = jpype.JClass("com.dask.sql.schema.DaskTable")
DaskSchemaClass = jpype.JClass("com.dask.sql.schema.DaskSchema")
RelationalAlgebraGeneratorClass = jpype.JClass("com.dask.sql.application.RelationalAlgebraGenerator")

tableJava = TableClass("my_table")
for order, column in enumerate(["a", "b"]):
    dataType = ColumnTypeClass.DOUBLE
    tableJava.addColumn(column, dataType)

schema = DaskSchemaClass("main")
schema.addTable(tableJava)

generator = RelationalAlgebraGeneratorClass(schema)

print(generator.getRelationalAlgebraString("SELECT SUM(a) FROM my_table GROUP BY b"))
