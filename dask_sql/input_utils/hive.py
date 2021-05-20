import ast
import logging
import os
import re
from functools import partial
from typing import Any, Union

import dask.dataframe as dd

try:
    from pyhive import hive
except ImportError:  # pragma: no cover
    hive = None

try:
    import sqlalchemy
except ImportError:
    sqlalchemy = None

from dask_sql.input_utils.base import BaseInputPlugin
from dask_sql.mappings import cast_column_type, sql_to_python_type, sql_to_python_value

logger = logging.Logger(__name__)


class HiveInputPlugin(BaseInputPlugin):
    """Input Plugin from Hive"""

    def is_correct_input(
        self, input_item: Any, table_name: str, format: str = None, **kwargs
    ):
        is_sqlalchemy_hive = sqlalchemy and isinstance(
            input_item, sqlalchemy.engine.base.Connection
        )
        is_hive_cursor = hive and isinstance(input_item, hive.Cursor)

        return is_sqlalchemy_hive or is_hive_cursor or format == "hive"

    def to_dc(
        self, input_item: Any, table_name: str, format: str = None, **kwargs
    ):  # pragma: no cover
        table_name = kwargs.pop("hive_table_name", table_name)
        schema = kwargs.pop("hive_schema_name", "default")

        parsed = self._parse_hive_table_description(input_item, schema, table_name)
        (
            column_information,
            table_information,
            storage_information,
            partition_information,
        ) = parsed

        logger.debug("Extracted hive information: ")
        logger.debug(f"column information: {column_information}")
        logger.debug(f"table information: {table_information}")
        logger.debug(f"storage information: {storage_information}")
        logger.debug(f"partition information: {partition_information}")

        # Convert column information
        column_information = {
            col: sql_to_python_type(col_type.upper())
            for col, col_type in column_information.items()
        }

        # Extract format information
        if "InputFormat" in storage_information:
            format = storage_information["InputFormat"].split(".")[-1]
        # databricks format is different, see https://github.com/nils-braun/dask-sql/issues/83
        elif "InputFormat" in table_information:
            format = table_information["InputFormat"].split(".")[-1]
        else:
            raise RuntimeError(
                "Do not understand the output of 'DESCRIBE FORMATTED <table>'"
            )

        if format == "TextInputFormat" or format == "SequenceFileInputFormat":
            storage_description = storage_information.get("Storage Desc Params", {})
            read_function = partial(
                dd.read_csv,
                sep=storage_description.get("field.delim", ","),
                header=None,
            )
        elif format == "ParquetInputFormat" or format == "MapredParquetInputFormat":
            read_function = dd.read_parquet
        elif format == "OrcInputFormat":
            read_function = dd.read_orc
        elif format == "JsonInputFormat":
            read_function = dd.read_json
        else:
            raise AttributeError(f"Do not understand hive's table format {format}")

        def _normalize(loc):
            if loc.startswith("dbfs:/") and not loc.startswith("dbfs://"):
                # dask (or better: fsspec) needs to have the URL in a specific form
                # starting with two // after the protocol
                loc = f"dbfs://{loc.lstrip('dbfs:')}"
            # file:// is not a known protocol
            loc = loc.lstrip("file:")
            # Only allow files which do not start with . or _
            # Especially, not allow the _SUCCESS files
            return os.path.join(loc, "[A-Za-z0-9-]*")

        def wrapped_read_function(location, column_information, **kwargs):
            location = _normalize(location)
            logger.debug(f"Reading in hive data from {location}")
            df = read_function(location, **kwargs)

            logger.debug(f"Applying column information: {column_information}")
            df = df.rename(columns=dict(zip(df.columns, column_information.keys())))

            for col, expected_type in column_information.items():
                df = cast_column_type(df, col, expected_type)

            return df

        if partition_information:
            partition_list = self._parse_hive_partition_description(
                input_item, schema, table_name
            )
            logger.debug(f"Reading in partitions from {partition_list}")

            tables = []
            for partition in partition_list:
                parsed = self._parse_hive_table_description(
                    input_item, schema, table_name, partition=partition
                )
                (
                    partition_column_information,
                    partition_table_information,
                    _,
                    _,
                ) = parsed

                partition_column_information = {
                    col: sql_to_python_type(col_type.upper())
                    for col, col_type in partition_column_information.items()
                }

                location = partition_table_information["Location"]
                table = wrapped_read_function(
                    location, partition_column_information, **kwargs
                )

                partition_values = partition_table_information["Partition Value"]
                partition_values = partition_values[1 : len(partition_values) - 1]
                partition_values = partition_values.split(",")

                logger.debug(
                    f"Applying additional partition information as columns: {partition_information}"
                )

                partition_id = 0
                for partition_key, partition_type in partition_information.items():
                    value = partition_values[partition_id]
                    value = sql_to_python_value(partition_type.upper(), value)
                    table[partition_key] = value

                    # partition_type = sql_to_python_type()
                    # table = cast_column_type(table, partition_key, partition_type)

                    partition_id += 1

                tables.append(table)

            return dd.concat(tables)

        location = table_information["Location"]
        df = wrapped_read_function(location, column_information, **kwargs)
        return df

    def _escape_partition(self, partition: str):  # pragma: no cover
        """
        Given a partition string like `key=value` escape the string properly for Hive.
         Wrap anything but digits in quotes. Don't wrap the column name.
        """
        contains_only_digits = re.compile(r"^\d+$")
        escaped_partition = []

        for partition_part in partition.split("/"):
            try:
                k, v = partition_part.split("=")
                if re.match(contains_only_digits, v):
                    escaped_value = v
                else:
                    escaped_value = f'"{v}"'
                escaped_partition.append(f"{k}={escaped_value}")
            except ValueError:
                logger.warning(f"{partition_part} didn't contain a `=`")
                escaped_partition.append(partition_part)

        return ",".join(escaped_partition)

    def _parse_hive_table_description(
        self,
        cursor: Union["sqlalchemy.engine.base.Connection", "hive.Cursor"],
        schema: str,
        table_name: str,
        partition: str = None,
    ):  # pragma: no cover
        """
        Extract all information from the output
        of the DESCRIBE FORMATTED call, which is unfortunately
        in a format not easily readable by machines.
        """
        cursor.execute(f"USE {schema}")
        if partition:
            partition = self._escape_partition(partition)
            result = self._fetch_all_results(
                cursor, f"DESCRIBE FORMATTED {table_name} PARTITION ({partition})"
            )
        else:
            result = self._fetch_all_results(cursor, f"DESCRIBE FORMATTED {table_name}")

        logger.debug(f"Got information from hive: {result}")

        table_information = {}
        column_information = {}  # using the fact that dicts are insertion ordered
        storage_information = {}
        partition_information = {}
        mode = "column"
        last_field = None

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
                if mode == "column":
                    column_information[key] = value
                    last_field = column_information[key]
                elif mode == "storage":
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

        return (
            column_information,
            table_information,
            storage_information,
            partition_information,
        )

    def _parse_hive_partition_description(
        self,
        cursor: Union["sqlalchemy.engine.base.Connection", "hive.Cursor"],
        schema: str,
        table_name: str,
    ):  # pragma: no cover
        """
        Extract all partition informaton for a given table
        """
        cursor.execute(f"USE {schema}")
        result = self._fetch_all_results(cursor, f"SHOW PARTITIONS {table_name}")

        return [row[0] for row in result]

    def _fetch_all_results(
        self,
        cursor: Union["sqlalchemy.engine.base.Connection", "hive.Cursor"],
        sql: str,
    ):  # pragma: no cover
        """
        The pyhive.Cursor and the sqlalchemy connection behave slightly different.
        The former has the fetchall method on the cursor,
        whereas the latter on the executed query.
        """
        result = cursor.execute(sql)

        try:
            return result.fetchall()
        except AttributeError:
            return cursor.fetchall()
