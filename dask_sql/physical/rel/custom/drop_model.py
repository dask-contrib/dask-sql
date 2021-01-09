import logging

from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.datacontainer import DataContainer

logger = logging.getLogger(__name__)


class DropModelPlugin(BaseRelPlugin):
    """
    Drop a model with given name.
    The SQL call looks like

        DROP MODEL <table-name>
    """

    class_name = "com.dask.sql.parser.SqlDropModel"

    def convert(
        self, sql: "org.apache.calcite.sql.SqlNode", context: "dask_sql.Context"
    ) -> DataContainer:
        model_name = str(sql.getModelName())

        if model_name not in context.models:
            if not sql.getIfExists():
                raise RuntimeError(
                    f"A model with the name {model_name} is not present."
                )
            else:
                return

        del context.models[model_name]
