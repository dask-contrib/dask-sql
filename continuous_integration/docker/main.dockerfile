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
COPY continuous_integration/docker/conda.txt /opt/dask_sql/
RUN mamba install -y \
    # build requirements
    "maturin>=1.3,<1.4" \
    # core dependencies
    "dask>=2022.3.0" \
    "pandas>=1.4.0" \
    "fastapi>=0.92.0" \
    "httpx>=0.24.1" \
    "uvicorn>=0.13.4" \
    "tzlocal>=2.1" \
    "prompt_toolkit>=3.0.8" \
    "pygments>=2.7.1" \
    tabulate \
    # additional dependencies
    "pyarrow>=14.0.1" \
    "scikit-learn>=1.0.0" \
    "intake>=0.6.0" \
    && conda clean -ay

# install dask-sql
COPY Cargo.toml /opt/dask_sql/
COPY Cargo.lock /opt/dask_sql/
COPY pyproject.toml /opt/dask_sql/
COPY setup.cfg /opt/dask_sql/
COPY README.md /opt/dask_sql/
COPY .git /opt/dask_sql/.git
COPY src /opt/dask_sql/src
COPY dask_sql /opt/dask_sql/dask_sql
RUN cd /opt/dask_sql/ \
    && CONDA_PREFIX="/opt/conda/" maturin develop

# Set the script to execute
COPY continuous_integration/scripts/startup_script.py /opt/dask_sql/startup_script.py

EXPOSE 8080
ENTRYPOINT [ "/usr/bin/prepare.sh", "/opt/conda/bin/python", "/opt/dask_sql/startup_script.py" ]
