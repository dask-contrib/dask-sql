# Dockerfile for dask-sql running the SQL server
# For more information, see https://dask-sql.readthedocs.io/.
FROM daskdev/dask:latest
LABEL author "Nils Braun <nilslennartbraun@gmail.com>"

# Install rustc & gcc for compilation of DataFusion planner
ADD https://sh.rustup.rs /rustup-init.sh
RUN sh /rustup-init.sh -y --default-toolchain=stable --profile=minimal \
    && apt-get update \
    && apt-get install gcc -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install conda dependencies for dask-sql
COPY docker/conda.txt /opt/dask_sql/
RUN mamba install -y \
    # build requirements
    "setuptools-rust>=1.4.1" \
    # core dependencies
    "dask>=2022.3.0,<=2023.1.1" \
    "pandas>=1.4.0" \
    # FIXME: handling is needed for httpx-based fastapi>=0.87.0
    "fastapi>=0.69.0,<0.87.0" \
    "uvicorn>=0.13.4" \
    "tzlocal>=2.1" \
    "prompt_toolkit>=3.0.8" \
    "pygments>=2.7.1" \
    tabulate \
    nest-asyncio \
    # additional dependencies
    "pyarrow>=6.0.1" \
    "scikit-learn>=1.0.0" \
    "intake>=0.6.0" \
    && conda clean -ay

# install dask-sql
COPY setup.py /opt/dask_sql/
COPY setup.cfg /opt/dask_sql/
COPY versioneer.py /opt/dask_sql/
COPY .git /opt/dask_sql/.git
COPY dask_planner /opt/dask_sql/dask_planner
COPY dask_sql /opt/dask_sql/dask_sql
RUN cd /opt/dask_sql/ \
    && pip install -e . -vv

# Set the script to execute
COPY scripts/startup_script.py /opt/dask_sql/startup_script.py

EXPOSE 8080
ENTRYPOINT [ "/usr/bin/prepare.sh", "/opt/conda/bin/python", "/opt/dask_sql/startup_script.py" ]
