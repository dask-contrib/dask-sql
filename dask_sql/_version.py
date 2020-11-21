from importlib.metadata import version, PackageNotFoundError


def get_version():
    try:
        return version("dask-sql")
    except PackageNotFoundError:  # pragma: no cover
        pass
