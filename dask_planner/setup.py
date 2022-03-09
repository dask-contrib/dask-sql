import distutils
import os
import shutil
import subprocess
import sys

from setuptools import find_namespace_packages, setup
from setuptools_rust import Binding, RustExtension

setup(
    name="dask_planner",
    version="0.0.1",
    packages=find_namespace_packages(include=["dask_planner.*"]),
    rust_extensions=[
        RustExtension(
            "dask_planner.rust", binding=Binding.PyO3, path="Cargo.toml", debug=False,
        )
    ],
    python_requires=">=3.8",
    zip_safe=False,
)
