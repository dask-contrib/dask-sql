from typing import Any

from dask_sql.input_utils.hive import HiveInputPlugin


class SqlalchemyHiveInputPlugin(HiveInputPlugin):
    """Input Plugin from sqlalchemy string"""

    def is_correct_input(
        self, input_item: Any, table_name: str, format: str = None, **kwargs
    ):
        correct_prefix = isinstance(input_item, str) and (
            input_item.startswith("hive://")
            or input_item.startswith("databricks+pyhive://")
        )
        return correct_prefix

    def to_dc(
        self,
        input_item: Any,
        table_name: str,
        format: str = None,
        gpu: bool = False,
        **kwargs
    ):  # pragma: no cover
        if gpu:
            raise NotImplementedError("Hive does not support gpu")

        import sqlalchemy

        engine_kwargs = {}
        if "connect_args" in kwargs:
            engine_kwargs["connect_args"] = kwargs.pop("connect_args")

        if format is not None:
            raise AttributeError(
                "Format specified and sqlalchemy connection string set!"
            )

        cursor = sqlalchemy.create_engine(input_item, **engine_kwargs).connect()
        return super().to_dc(cursor, table_name=table_name, **kwargs)
