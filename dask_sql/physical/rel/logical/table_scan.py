import logging
import operator
from functools import reduce
from typing import TYPE_CHECKING

import dask_cudf as ddf
import numpy as np
import pandas as pd
from dask.utils_test import hlg_layer

from dask_sql.datacontainer import ColumnContainer, DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.rel.logical.filter import filter_or_scalar
from dask_sql.physical.rex import RexConverter

if TYPE_CHECKING:
    import dask_sql
    from dask_planner.rust import LogicalPlan

logger = logging.getLogger(__name__)


class DaskTableScanPlugin(BaseRelPlugin):
    """
    A DaskTableScan is the main ingredient: it will get the data
    from the database. It is always used, when the SQL looks like

        SELECT .... FROM table ....

    We need to get the dask dataframe from the registered
    tables and return the requested columns from it.
    """

    class_name = "TableScan"

    def convert(
        self,
        rel: "LogicalPlan",
        context: "dask_sql.Context",
    ) -> DataContainer:
        # There should not be any input. This is the first step.
        self.assert_inputs(rel, 0)

        # Rust table_scan instance handle
        table_scan = rel.table_scan()

        # The table(s) we need to return
        dask_table = rel.getTable()
        schema_name, table_name = [n.lower() for n in context.fqn(dask_table)]

        dc = context.schema[schema_name].tables[table_name]

        # Apply filter before projections since filter columns may not be in projections
        dc = self._apply_filters(table_scan, rel, dc, context)
        dc = self._apply_projections(table_scan, dask_table, dc)

        cc = dc.column_container
        cc = self.fix_column_to_row_type(cc, rel.getRowType())
        dc = DataContainer(dc.df, cc)
        dc = self.fix_dtype_to_row_type(dc, rel.getRowType())
        return dc

    def _apply_projections(self, table_scan, dask_table, dc):
        # If the 'TableScan' instance contains projected columns only retrieve those columns
        # otherwise get all projected columns from the 'Projection' instance, which is contained
        # in the 'RelDataType' instance, aka 'row_type'
        df = dc.df
        cc = dc.column_container
        if table_scan.containsProjections():
            field_specifications = (
                table_scan.getTableScanProjects()
            )  # Assumes these are column projections only and field names match table column names
            df = df[field_specifications]
        else:
            field_specifications = [
                str(f) for f in dask_table.getRowType().getFieldNames()
            ]
        cc = cc.limit_to(field_specifications)
        return DataContainer(df, cc)

    def _apply_filters(self, table_scan, rel, dc, context):

        # Columns that should be projected
        cols = table_scan.getTableScanProjects()

        filters = table_scan.getFilters()
        if filters:
            df = dc.df

            # Generate the filters in DNF form for the cudf reader
            filtered_result = table_scan.getDNFFilters()
            filtered = filtered_result.filtered_exprs
            unfiltered = filtered_result.io_unfilterable_exprs

            if len(filtered) > 0:
                # Prepare the filters to be in the format expected by Python since they came from Rust
                updated_filters = []
                for filter_tup in filtered:
                    if filter_tup[2].startswith("Int"):
                        num = filter_tup[2].split("(")[1].split(")")[0]
                        updated_filters.append((filter_tup[0], filter_tup[1], int(num)))
                    elif filter_tup[2].startswith("TimestampNanosecond"):
                        ns_timestamp = filter_tup[2].split("(")[1].split(")")[0]
                        val = pd.to_datetime(ns_timestamp, unit="ns")
                        updated_filters.append((filter_tup[0], filter_tup[1], val))
                    elif filter_tup[2] == "np.nan":
                        updated_filters.append((filter_tup[0], filter_tup[1], np.nan))
                    else:
                        updated_filters.append(filter_tup)

                # Rebuild the read_* operation from the existing Dask task
                # TODO: This will currently only work with parquet, need to update
                layer = hlg_layer(df.dask, "read-parquet")
                creation_path = layer.creation_info["args"][0]
                print(
                    f"Rebuilding Dask Task `read_parquet()` \n \
                    Original Dask read-parquet: {layer} \n \
                    Original creation_info: {layer.creation_info} \n \
                    Path: {creation_path} \n \
                    Filters: {updated_filters} \n \
                    Columns: {cols} \n \
                    split_row_groups: {layer.creation_info['kwargs']['split_row_groups']} \n \
                    aggregate_files: {layer.creation_info['kwargs']['aggregate_files']}\n"
                )

                df = ddf.read_parquet(
                    creation_path,
                    filters=updated_filters,
                    columns=cols,
                    split_row_groups=layer.creation_info["kwargs"]["split_row_groups"],
                    aggregate_files=layer.creation_info["kwargs"]["aggregate_files"],
                )
                dc = DataContainer(df, ColumnContainer(df.columns))

            # All partial filters here are applied in conjunction (&)
            if len(unfiltered) > 0:
                df_condition = reduce(
                    operator.and_,
                    [
                        RexConverter.convert(rel, rex, dc, context=context)
                        for rex in unfiltered
                    ],
                )
                df = filter_or_scalar(df, df_condition)

            dc = DataContainer(df, ColumnContainer(df.columns))

        return dc
