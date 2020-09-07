import distutils
from setuptools import setup, find_packages
import setuptools.command.build_py
import os
import shutil
import subprocess


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
        command = ["mvn", "clean", "install", "-f", "pom.xml"]
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


with open("README.md") as f:
    long_description = f.read()


setup(
    name="dask_sql",
    use_scm_version=True,
    description="Dask SQL",
    url="http://github.com/nils-braun/dask-sql/",
    maintainer="Nils Braun",
    maintainer_email="nilslennartbraun@gmail.com",
    license="MIT",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(include=["dask_sql", "dask_sql.*"]),
    package_data={"dask_sql": ["jar/DaskSQL.jar"]},
    python_requires=">=3.6",
    setup_requires=["setuptools_scm"],
    install_requires=["dask[dataframe]>=2.19.0", "jpype1>=1.0.2"],
    zip_safe=False,
    cmdclass={"java": MavenCommand, "build_py": BuildPyCommand},
)
