import jpype


jpype.addClassPath("../planner/application/target/DaskSQL.jar")
jpype.startJVM("-ea", convertStrings=False)


DaskTable = jpype.JClass("com.dask.sql.schema.DaskTable")
DaskSchema = jpype.JClass("com.dask.sql.schema.DaskSchema")
RelationalAlgebraGenerator = jpype.JClass("com.dask.sql.application.RelationalAlgebraGenerator")
SqlTypeName = jpype.JClass("org.apache.calcite.sql.type.SqlTypeName")