import logging
from typing import TYPE_CHECKING

from dask_sql.datacontainer import DataContainer
from dask_sql.physical.rel.base import BaseRelPlugin

if TYPE_CHECKING:
    import dask_sql
    from dask_sql.rust import LogicalPlan

logger = logging.getLogger(__name__)


class DropModelPlugin(BaseRelPlugin):
    """
    Drop a model with given name.
    The SQL call looks like

        DROP MODEL <table-name>
    """

    class_name = "DropModel"

    def convert(self, rel: "LogicalPlan", context: "dask_sql.Context") -> DataContainer:
        drop_model = rel.drop_model()

        schema_name, model_name = context.schema_name, drop_model.getModelName()

        if model_name not in context.schema[schema_name].models:
            if not drop_model.getIfExists():
                raise RuntimeError(
                    f"A model with the name {model_name} is not present."
                )
            else:
                return

        del context.schema[schema_name].models[model_name]
