ARG DOCKER_META_VERSION
FROM nbraun/dask-sql:${DOCKER_META_VERSION}

RUN conda config --add channels conda-forge \
    && /opt/conda/bin/conda install --freeze-installed \
    "s3fs" \
    "dask-cloudprovider" \
    && pip install awscli \
    && conda clean -ay

ENTRYPOINT ["tini", "-g", "--", "/usr/bin/prepare.sh"]
