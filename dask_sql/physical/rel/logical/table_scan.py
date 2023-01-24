import logging
import operator
from functools import reduce
from typing import TYPE_CHECKING

from dask_sql.datacontainer import DataContainer, ColumnContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.rel.logical.filter import filter_or_scalar
from dask_sql.physical.rex import RexConverter

import numpy as np

import dask_cudf as ddf

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
        
        dc = self._apply_filters(context.schema[schema_name].tables_meta[table_name], table_scan, rel, context)
        # Apply filter before projections since filter columns may not be in projections
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

    def _apply_filters(self, tbl_meta, table_scan, rel, context):
        # df = dc.df
        # cc = dc.column_container
        # Columns that should be projected
        cols = table_scan.getTableScanProjects()

        filters = table_scan.getFilters()
        if filters:
            # Generate the filters in DNF form for the cudf reader
            filtered_result = table_scan.getDNFFilters()
            print(f"Filtered Result: {filtered_result}")
            filtered = filtered_result.filtered_exprs
            unfiltered = filtered_result.io_unfilterable_exprs
            print(f"Filtered: {filtered}")
            print(f"Un-filtered: {unfiltered}")

            if len(filtered) > 0:
                # Prepare the filters to be in the format expected by Python since they came from Rust
                updated_filters = []
                for filter_tup in filtered:
                    if filter_tup[2].startswith("Int"):
                        num = filter_tup[2].split('(')[1].split(')')[0]
                        updated_filters.append((filter_tup[0], filter_tup[1], int(num)))
                    elif filter_tup[2] == "np.nan":
                        updated_filters.append((filter_tup[0], filter_tup[1], np.nan))
                    else:
                        updated_filters.append(filter_tup)

                print(f"Invoking ddf.read_parquet with filters: {updated_filters}")

                df = ddf.read_parquet(tbl_meta["input_path"], filters=updated_filters, columns=cols)
            else:
                df = ddf.read_parquet(tbl_meta["input_path"], columns=cols)

            dc = DataContainer(df.copy(), ColumnContainer(df.columns))

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
        else:
            df = ddf.read_parquet(tbl_meta["input_path"], columns=cols)

        return DataContainer(df, ColumnContainer(df.columns))
