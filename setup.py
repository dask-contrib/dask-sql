import os
import sys

from setuptools import find_namespace_packages, find_packages, setup
from setuptools_rust import Binding, RustExtension

import versioneer

long_description = ""
if os.path.exists("README.md"):
    with open("README.md") as f:
        long_description = f.read()

needs_sphinx = "build_sphinx" in sys.argv
sphinx_requirements = ["sphinx>=3.2.1", "sphinx_rtd_theme"] if needs_sphinx else []

cmdclass = versioneer.get_cmdclass()

setup(
    name="dask_planner",
    version="0.0.1",
    packages=find_namespace_packages(include=["dask_planner.*"]),
    rust_extensions=[
        RustExtension(
            "dask_planner.rust",
            binding=Binding.PyO3,
            path="dask_planner/Cargo.toml",
            debug=False,
        )
    ],
    python_requires=">=3.8",
    zip_safe=False,
)

setup(
    name="dask_sql",
    version=versioneer.get_version(),
    description="SQL query layer for Dask",
    url="https://github.com/dask-contrib/dask-sql/",
    maintainer="Nils Braun",
    maintainer_email="nilslennartbraun@gmail.com",
    license="MIT",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(
        include=["dask_sql", "dask_sql.*", "dask_planner", "dask_planner.*"]
    ),
    python_requires=">=3.8",
    setup_requires=sphinx_requirements,
    install_requires=[
        "dask[dataframe,distributed]>=2022.3.0",
        "pandas>=1.0.0",  # below 1.0, there were no nullable ext. types
        "fastapi>=0.61.1",
        "uvicorn>=0.11.3",
        "tzlocal>=2.1",
        "prompt_toolkit",
        "pygments",
        "tabulate",
        "nest-asyncio",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0.1",
            "pytest-cov>=2.10.1",
            "mock>=4.0.3",
            "sphinx>=3.2.1",
            "pyarrow>=0.15.1",
            "dask-ml>=2022.1.22",
            "scikit-learn>=0.24.2",
            "intake>=0.6.0",
            "pre-commit",
            "black==22.3.0",
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
    cmdclass=cmdclass,
    command_options={
        "build_sphinx": {
            "source_dir": ("setup.py", "docs"),
        }
    },
)
