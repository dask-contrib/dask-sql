from setuptools import setup, find_packages
import os

setup(name='dask_sql',
      version='0.1.0',
      description='Dask SQL',
      url='http://github.com/nils-braun/dask-sql/',
      maintainer='Nils Braun',
      maintainer_email='nilslennartbraun@gmail.com',
      # license='BSD',
      packages=find_packages(include=["dask_sql", "dask_sql.*"]),
      python_requires='>=3.5',
      long_description=(open('README.rst').read() if os.path.exists('README.rst') else ''),
      install_requires=["dask"],
      zip_safe=False
)
