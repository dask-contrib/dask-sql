ARG DOCKER_META_VERSION
FROM nbraun/dask-sql:${DOCKER_META_VERSION}

RUN conda config --add channels conda-forge \
    && /opt/conda/bin/mamba install --freeze-installed \
    "s3fs" \
    "dask-cloudprovider" -y \
    && pip install awscli \
    && conda clean -ay

ENTRYPOINT ["tini", "-g", "--", "/usr/bin/prepare.sh"]
