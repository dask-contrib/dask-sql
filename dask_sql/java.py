"""
This file summarizes all java accessm dask_sql needs.

The jpype package is used to access Java classes from python.
It needs to know the java class path, which is set
to the jar file in the package resources.
"""
import pkg_resources
import logging

import jpype

from dask_sql.utils import _set_or_check_java_home

logger = logging.getLogger(__name__)

try:  # pragma: no cover
    # If using dask-sql together with hdfs input
    # (e.g. via dask), we have two concurring components
    # accessing the Java VM: the hdfs FS implementation
    # and dask-sql. hdfs needs to have the hadoop libraries
    # in the classpath and has a nice helper function for that
    # Unfortunately, this classpath will not be picked up if
    # set after the JVM is already running. So we need to make
    # sure we call it in all circumstances before starting the
    # JVM.
    from pyarrow.hdfs import _maybe_set_hadoop_classpath

    _maybe_set_hadoop_classpath()
except:  # pragma: no cover
    pass

# Define how to run the java virtual machine.
jpype.addClassPath(pkg_resources.resource_filename("dask_sql", "jar/DaskSQL.jar"))

_set_or_check_java_home()
jvmpath = jpype.getDefaultJVMPath()

# There seems to be a bug on Windows server for java >= 11 installed via conda
# It uses a wrong java class path, which can be easily recognizes
# by the \\bin\\bin part. We fix this here.
jvmpath = jvmpath.replace("\\bin\\bin\\server\\jvm.dll", "\\bin\\server\\jvm.dll")

logger.debug(f"Starting JVM from path {jvmpath}...")
jpype.startJVM(
    "-ea",
    "--illegal-access=deny",
    ignoreUnrecognized=True,
    convertStrings=False,
    jvmpath=jvmpath,
)
logger.debug("...having started JVM")


# Some Java classes we need
DaskTable = jpype.JClass("com.dask.sql.schema.DaskTable")
DaskAggregateFunction = jpype.JClass("com.dask.sql.schema.DaskAggregateFunction")
DaskScalarFunction = jpype.JClass("com.dask.sql.schema.DaskScalarFunction")
DaskSchema = jpype.JClass("com.dask.sql.schema.DaskSchema")
RelationalAlgebraGenerator = jpype.JClass(
    "com.dask.sql.application.RelationalAlgebraGenerator"
)
SqlTypeName = jpype.JClass("org.apache.calcite.sql.type.SqlTypeName")
List = jpype.JClass("java.util.List")
ValidationException = jpype.JClass("org.apache.calcite.tools.ValidationException")
SqlParseException = jpype.JClass("org.apache.calcite.sql.parser.SqlParseException")


def get_java_class(instance):
    """Get the stringified class name of a java object"""
    return str(instance.getClass().getName())


def get_short_java_class(instance):
    """Get only the last part of the class of a java object, after the last ."""
    return get_java_class(instance).split(".")[-1]
