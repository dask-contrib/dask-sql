import pkg_resources

import jpype


jpype.addClassPath(pkg_resources.resource_filename('dask_sql', 'jar/DaskSQL.jar'))
jpype.startJVM("-ea", convertStrings=False)


DaskTable = jpype.JClass("com.dask.sql.schema.DaskTable")
DaskSchema = jpype.JClass("com.dask.sql.schema.DaskSchema")
RelationalAlgebraGenerator = jpype.JClass(
    "com.dask.sql.application.RelationalAlgebraGenerator"
)
SqlTypeName = jpype.JClass("org.apache.calcite.sql.type.SqlTypeName")
List = jpype.JClass("java.util.List")


def get_java_class(instance):
    return str(instance.getClass().getName())


def get_short_java_class(instance):
    return get_java_class(instance).split(".")[-1]
