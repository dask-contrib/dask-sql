import distutils
import os
import shutil
import subprocess
import sys

import setuptools.command.build_py
from setuptools import find_packages, setup


class MavenCommand(distutils.cmd.Command):
    """Run the maven build command"""

    description = "run the mvn install command"
    user_options = []

    def initialize_options(self):
        """No options"""
        pass

    def finalize_options(self):
        """No options"""
        pass

    def run(self):
        """Run the mvn installation command"""
        # We need to explicitely specify the full path to mvn
        # for Windows
        maven_command = shutil.which("mvn")
        if not maven_command:
            raise OSError(
                "Can not find the mvn (maven) binary. Make sure to install maven before building the jar."
            )
        command = [maven_command, "clean", "install", "-f", "pom.xml"]
        self.announce(f"Running command: {' '.join(command)}", level=distutils.log.INFO)

        subprocess.check_call(command, cwd="planner")

        # Copy the artifact. We could also make maven do that,
        # but in this way we have full control from python
        os.makedirs("dask_sql/jar", exist_ok=True)
        shutil.copy("planner/target/DaskSQL.jar", "dask_sql/jar/DaskSQL.jar")


class BuildPyCommand(setuptools.command.build_py.build_py):
    """Customize the build command and add the java build"""

    def run(self):
        """Run the maven build before the normal build"""
        self.run_command("java")
        setuptools.command.build_py.build_py.run(self)


long_description = ""
if os.path.exists("README.md"):
    with open("README.md") as f:
        long_description = f.read()

needs_sphinx = "build_sphinx" in sys.argv
sphinx_requirements = ["sphinx>=3.2.1", "sphinx_rtd_theme"] if needs_sphinx else []

setup(
    name="dask_sql",
    description="Dask SQL",
    url="http://github.com/nils-braun/dask-sql/",
    maintainer="Nils Braun",
    maintainer_email="nilslennartbraun@gmail.com",
    license="MIT",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(include=["dask_sql", "dask_sql.*"]),
    package_data={"dask_sql": ["jar/DaskSQL.jar"]},
    use_scm_version=True,
    python_requires=">=3.6",
    setup_requires=["setuptools_scm"] + sphinx_requirements,
    install_requires=[
        "dask[dataframe]>=2.19.0,<=2021.2.0",  # dask 2021.3.0 makes
        # dask-ml fail (see https://github.com/dask/dask-ml/issues/803)
        # and introduces a different quantile method (which so far
        # gives unreasonable results)
        "pandas<1.2.0,>=1.0.0",  # pandas 1.2.0 introduced float NaN dtype,
        # which is currently not working with dask,
        # so the test is failing, see https://github.com/dask/dask/issues/7156
        # below 1.0, there were no nullable ext. types
        "jpype1>=1.0.2",
        "fastapi>=0.61.1",
        "uvicorn>=0.11.3",
        "tzlocal>=2.1",
        "prompt_toolkit",
        "pygments",
        # backport for python versions without importlib.metadata
        "importlib_metadata; python_version < '3.8.0'",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0.1",
            "pytest-cov>=2.10.1",
            "mock>=4.0.3",
            "sphinx>=3.2.1",
            "pyarrow>=0.15.1",
            "dask-ml>=1.7.0",
            "scikit-learn<0.24.0",
            "intake>=0.6.0",
            "pre-commit",
            "black==19.10b0",
            "isort==5.7.0",
        ],
        "fugue": ["fugue[sql]>=0.5.3"],
    },
    entry_points={
        "console_scripts": [
            "dask-sql-server = dask_sql.server.app:main",
            "dask-sql = dask_sql.cmd:main",
        ]
    },
    zip_safe=False,
    cmdclass={"java": MavenCommand, "build_py": BuildPyCommand,},
    command_options={"build_sphinx": {"source_dir": ("setup.py", "docs"),}},
)
