"""
This file summarizes all java accessm dask_sql needs.

The jpype package is used to access Java classes from python.
It needs to know the java class path, which is set
to the jar file in the package resources.
"""
import pkg_resources

import jpype


# Define how to run the java virtual machine.
jpype.addClassPath(pkg_resources.resource_filename("dask_sql", "jar/DaskSQL.jar"))

# There seems to be a bug on Windows server for java >= 11 installed via conda
# It uses a wrong java class path, which can be easily recognizes
# by the \\bin\\bin part. We fix this here.
jvmpath = jpype.getDefaultJVMPath()
jvmpath = jvmpath.replace("\\bin\\bin\\server\\jvm.dll", "\\bin\\server\\jvm.dll")

jpype.startJVM(
    "-ea",
    "--illegal-access=deny",
    ignoreUnrecognized=True,
    convertStrings=False,
    jvmpath=jvmpath,
)


# Some Java classes we need
DaskTable = jpype.JClass("com.dask.sql.schema.DaskTable")
DaskSchema = jpype.JClass("com.dask.sql.schema.DaskSchema")
RelationalAlgebraGenerator = jpype.JClass(
    "com.dask.sql.application.RelationalAlgebraGenerator"
)
SqlTypeName = jpype.JClass("org.apache.calcite.sql.type.SqlTypeName")
List = jpype.JClass("java.util.List")


def get_java_class(instance):
    """Get the stringified class name of a java object"""
    return str(instance.getClass().getName())


def get_short_java_class(instance):
    """Get only the last part of the class of a java object, after the last ."""
    return get_java_class(instance).split(".")[-1]
