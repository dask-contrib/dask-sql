# Dockerfile for dask-sql running the SQL server
# For more information, see https://dask-sql.readthedocs.io/.
FROM daskdev/dask:2.30.0
LABEL author "Nils Braun <nilslennartbraun@gmail.com>"

# Install dependencies for dask-sql
COPY conda.yaml /opt/dask_sql/
RUN /opt/conda/bin/conda install \
    --file /opt/dask_sql/conda.yaml \
    -c conda-forge

# Build the java libraries
COPY setup.py /opt/dask_sql/
COPY .git /opt/dask_sql/.git
COPY planner /opt/dask_sql/planner
RUN cd /opt/dask_sql/ \
    && python setup.py java

# Install the python library
COPY dask_sql /opt/dask_sql/dask_sql
RUN cd /opt/dask_sql/ \
    && pip install -e .

# Set the script to execute
COPY scripts/startup_script.py /opt/dask_sql/startup_script.py

EXPOSE 8080
ENV JAVA_HOME /opt/conda
ENTRYPOINT [ "/usr/bin/prepare.sh", "/opt/conda/bin/python", "/opt/dask_sql/startup_script.py" ]
