"""
This file summarizes all java accessm dask_sql needs.

The jpype package is used to access Java classes from python.
It needs to know the java class path, which is set
to the jar file in the package resources.
"""
import logging
import os
import pkg_resources
import platform
import warnings

import jpype

logger = logging.getLogger(__name__)


def _set_or_check_java_home():
    """
    We have some assumptions on the JAVA_HOME, namely our jvm comes from
    a conda environment. That does not need to be true, but we should at
    least warn the user.
    """
    if "CONDA_PREFIX" not in os.environ:  # pragma: no cover
        # we are not running in a conda env
        return

    correct_java_path = os.path.normpath(os.environ["CONDA_PREFIX"])
    if platform.system() == "Windows":  # pragma: no cover
        correct_java_path = os.path.normpath(os.path.join(correct_java_path, "Library"))

    if "JAVA_HOME" not in os.environ:  # pragma: no cover
        logger.debug("Setting $JAVA_HOME to $CONDA_PREFIX")
        os.environ["JAVA_HOME"] = correct_java_path
    elif (
        os.path.normpath(os.environ["JAVA_HOME"]) != correct_java_path
    ):  # pragma: no cover
        warnings.warn(
            "You are running in a conda environment, but the JAVA_PATH is not using it. "
            f"If this is by mistake, set $JAVA_HOME to {correct_java_path}, instead of {os.environ['JAVA_HOME']}."
        )


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
com = jpype.JPackage("com")
org = jpype.JPackage("org")
java = jpype.JPackage("java")

DaskTable = com.dask.sql.schema.DaskTable
DaskAggregateFunction = com.dask.sql.schema.DaskAggregateFunction
DaskScalarFunction = com.dask.sql.schema.DaskScalarFunction
DaskSchema = com.dask.sql.schema.DaskSchema
RelationalAlgebraGenerator = com.dask.sql.application.RelationalAlgebraGenerator
SqlTypeName = org.apache.calcite.sql.type.SqlTypeName
ValidationException = org.apache.calcite.tools.ValidationException
SqlParseException = org.apache.calcite.sql.parser.SqlParseException


def get_java_class(instance):
    """Get the stringified class name of a java object"""
    return str(instance.getClass().getName())
