import os
from functools import partial
import logging
from typing import Union

import dask.dataframe as dd
from distributed.client import default_client
import pandas as pd

try:
    from pyhive import hive
except ImportError:
    hive = None

from dask_sql.datacontainer import DataContainer, ColumnContainer

logger = logging.Logger(__name__)

InputType = Union[dd.DataFrame, pd.DataFrame, str, "hive.Cursor"]


def to_dc(
    input_item: InputType,
    file_format: str = None,
    persist: bool = True,
    hive_table_name: str = None,
    hive_schema_name: str = "default",
    **kwargs,
) -> DataContainer:
    """
    Turn possible input descriptions or formats (e.g. dask dataframes, pandas dataframes,
    locations as string, hive tables) into the loaded data containers,
    maybe persist them to cluster memory before.
    """
    filled_get_dask_dataframe = lambda *args: _get_dask_dataframe(
        *args,
        file_format=file_format,
        hive_table_name=hive_table_name,
        hive_schema_name=hive_schema_name,
        **kwargs,
    )

    if isinstance(input_item, list):
        table = dd.concat([filled_get_dask_dataframe(item) for item in input_item])
    else:
        table = filled_get_dask_dataframe(input_item)

    if persist:
        table = table.persist()

    return DataContainer(table.copy(), ColumnContainer(table.columns))


def _get_dask_dataframe(
    input_item: InputType,
    file_format: str = None,
    hive_table_name: str = None,
    hive_schema_name: str = "default",
    **kwargs,
):
    if isinstance(input_item, pd.DataFrame):
        return dd.from_pandas(input_item, npartitions=1, **kwargs)
    elif isinstance(input_item, dd.DataFrame):
        return input_item
    elif hive and isinstance(input_item, hive.Cursor):  # pragma: no cover
        return _get_files_from_hive(
            input_item, table_name=hive_table_name, schema=hive_schema_name, **kwargs
        )
    elif isinstance(input_item, str):
        return _get_files_from_location(input_item, file_format=file_format, **kwargs)
    else:  # pragma: no cover
        raise ValueError(f"Do not understand the input type {type(input_item)}")


def _get_files_from_location(input_item, file_format: str = None, **kwargs):
    if file_format == "memory":
        client = default_client()
        return client.get_dataset(input_item, **kwargs)

    if not file_format:
        _, extension = os.path.splitext(input_item)

        file_format = extension.lstrip(".")

    try:
        read_function = getattr(dd, f"read_{file_format}")
    except AttributeError:
        raise AttributeError(f"Can not read files of format {file_format}")

    return read_function(input_item, **kwargs)


def _get_files_from_hive(
    cursor: "hive.Cursor", table_name: str, schema: str = "default", **kwargs
):  # pragma: no cover
    (
        table_information,
        storage_information,
        partition_information,
    ) = _parse_hive_table_description(cursor, schema, table_name)

    if table_information["Table Type"] != "EXTERNAL_TABLE":
        raise AssertionError("Can only read in EXTERNAL TABLES")

    format = storage_information["InputFormat"].split(".")[-1]
    storage_description = storage_information["Storage Desc Params"]

    if format == "TextInputFormat":
        read_function = partial(
            dd.read_csv, sep=storage_description.get("field.delim", ",")
        )
    elif format == "ParquetInputFormat":
        read_function = dd.read_parquet
    elif format == "OrcInputFormat":
        read_function = dd.read_orc
    elif format == "JsonInputFormat":
        read_function = dd.read_json
    else:
        raise AttributeError(f"Do not understand hive's table format {format}")

    def _normalize(loc):
        return os.path.join(loc.lstrip("file:"), "**")

    if partition_information:
        partition_information = _parse_hive_partition_description(
            cursor, schema, table_name
        )

        tables = []
        for partition in partition_information:
            (table_information, _, _) = _parse_hive_table_description(
                cursor, schema, table_name, partition=partition
            )
            location = _normalize(table_information["Location"])
            table = read_function(location, **kwargs)

            # TODO: insert partition info into dataframe?
            tables.append(table)

        return dd.concat(tables)

    location = _normalize(table_information["Location"])
    return read_function(location, **kwargs)


def _parse_hive_table_description(
    cursor: "hive.Cursor", schema: str, table_name: str, partition: str = None
):  # pragma: no cover
    """
    Extract all information from the output
    of the DESCRIBE FORMATTED call, which is unfortunately
    in a format not easily readable by machines.
    """
    cursor.execute(f"USE {schema}")
    if partition:
        cursor.execute(f"DESCRIBE FORMATTED {table_name} PARTITION ({partition})")
    else:
        cursor.execute(f"DESCRIBE FORMATTED {table_name}")

    result = cursor.fetchall()
    logger.debug(f"Got information from hive: {result}")

    table_information = {}
    storage_information = {}
    partition_information = {}
    mode = None
    last_field = None

    from pprint import pprint

    for key, value, value2 in result:
        key = key.strip().rstrip(":") if key else ""
        value = value.strip() if value else ""
        value2 = value2.strip() if value2 else ""

        # That is just a comment line, we can skip it
        if key == "# col_name":
            continue

        if (
            key == "# Detailed Table Information"
            or key == "# Detailed Partition Information"
        ):
            mode = "table"
        elif key == "# Storage Information":
            mode = "storage"
        elif key == "# Partition Information":
            mode = "partition"
        elif key.startswith("#"):
            mode = None
        elif key:
            if not value:
                value = dict()
            if mode == "storage":
                storage_information[key] = value
                last_field = storage_information[key]
            elif mode == "table":
                table_information[key] = value
                last_field = table_information[key]
            elif mode == "partition":
                partition_information[key] = value
                last_field = partition_information[key]
        elif value and last_field is not None:
            last_field[value] = value2

    return table_information, storage_information, partition_information


def _parse_hive_partition_description(
    cursor: "hive.Cursor", schema: str, table_name: str
):  # pragma: no cover
    """
    Extract all partition informaton for a given table
    """
    cursor.execute(f"USE {schema}")
    cursor.execute(f"SHOW PARTITIONS {table_name}")

    result = cursor.fetchall()

    return [row[0] for row in result]
