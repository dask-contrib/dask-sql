try:
    from importlib import metadata
except ImportError:  # for Python < 3.8
    import importlib_metadata as metadata


def get_version():
    try:
        return metadata.version("dask-sql")
    except metadata.PackageNotFoundError:  # pragma: no cover
        pass
