.. _installation:

Installation
============

``dask-sql`` can be installed via ``conda`` (preferred) or ``pip`` - or in a development environment.

You can continue with the :ref:`quickstart` after the installation.

With ``conda``
--------------

Create a new conda environment or use your already present environment:

.. code-block:: bash

    conda create -n dask-sql
    conda activate dask-sql

Install the package from the ``conda-forge`` channel:

.. code-block:: bash

    conda install dask-sql -c conda-forge

With ``pip``
------------

``dask-sql`` needs Java for the parsing of the SQL queries.
Before installation, make sure you have a running java installation with version >= 8.

To test if you have Java properly installed and set up, run

.. code-block:: bash

    $ java -version
    openjdk version "1.8.0_152-release"
    OpenJDK Runtime Environment (build 1.8.0_152-release-1056-b12)
    OpenJDK 64-Bit Server VM (build 25.152-b12, mixed mode)

After installing Java, you can install the package with

.. code-block:: bash

    pip install dask-sql

For development
---------------

If you want to have the newest (unreleased) ``dask-sql`` version or if you plan to do development on ``dask-sql``, you can also install the package from sources.

.. code-block:: bash

    git clone https://github.com/nils-braun/dask-sql.git

Create a new conda environment and install the development environment:

.. code-block:: bash

    conda create -n dask-sql --file conda.yaml -c conda-forge

After that, you can install the package in development mode

.. code-block:: bash

    pip install -e .

This will also compile the Java classes.
If there were changes to the Java code, you need to rerun this compilation with

.. code-block:: bash

    python setup.py java

You can run the tests (after installation) with

.. code-block:: bash

    pytest tests