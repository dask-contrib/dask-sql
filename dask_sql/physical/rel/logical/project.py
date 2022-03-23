import logging
from typing import TYPE_CHECKING

import dask_planner
from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.physical.rex import RexConverter
from dask_sql.utils import new_temporary_column

if TYPE_CHECKING:
    import dask_sql

from dask_planner.rust import LogicalPlan, LogicalPlanGenerator

logger = logging.getLogger(__name__)


class DaskProjectPlugin(BaseRelPlugin):
    """
    A DaskProject is used to
    (a) apply expressions to the columns and
    (b) only select a subset of the columns
    """

    class_name = "Projection"

    def convert(
        self,
        dc: DataContainer,
        logical_generator: LogicalPlanGenerator,
        rel: LogicalPlan,
        context: "dask_sql.Context"
    ) -> DataContainer:
        df = dc.df
        cc = dc.column_container

        # The table(s) we need to return
        table = rel.table()

        # Collect all (new) columns
        named_projects = logical_generator.get_named_projects()

        column_names = []
        new_columns = {}
        new_mappings = {}

        for expr in named_projects:
            key = str(expr.column_name())
            column_names.append(key)

            random_name = new_temporary_column(df)
            new_columns[random_name] = df['a']
            logger.debug(f"Adding a new column {key} out of {expr}")
            new_mappings[key] = random_name

        # Actually add the new columns
        if new_columns:
            df = df.assign(**new_columns)

        # Make sure the order is correct
        cc = cc.limit_to(column_names)

        dc = DataContainer(df, cc)

        return dc
