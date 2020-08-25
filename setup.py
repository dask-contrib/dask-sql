import distutils
from setuptools import setup, find_packages
import os
import shutil
import subprocess


class MavenCommand(distutils.cmd.Command):
    """Run the maven build command"""

    description = "run the mvn install command"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        command = ["mvn", "clean", "install", "-f", "pom.xml"]
        self.announce(f"Running command: {' '.join(command)}", level=distutils.log.INFO)

        subprocess.check_call(command, cwd="planner")

        os.makedirs("dask_sql/jar", exist_ok=True)
        shutil.copy("planner/target/DaskSQL.jar", "dask_sql/jar/DaskSQL.jar")


setup(
    name="dask_sql",
    version="0.1.0",
    description="Dask SQL",
    url="http://github.com/nils-braun/dask-sql/",
    maintainer="Nils Braun",
    maintainer_email="nilslennartbraun@gmail.com",
    # license='BSD',
    packages=find_packages(include=["dask_sql", "dask_sql.*"]),
    package_data={'dask_sql': ["jar/DaskSQL.jar"]},
    python_requires=">=3.5",
    long_description=(
        open("README.rst").read() if os.path.exists("README.rst") else ""
    ),
    install_requires=["dask"],
    zip_safe=False,
    cmdclass={"java": MavenCommand,},
)
