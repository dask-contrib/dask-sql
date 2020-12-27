from dask_sql.physical.rel.custom.predict import PredictModelPlugin
from .create import CreateTablePlugin
from .create_as import CreateAsPlugin
from .columns import ShowColumnsPlugin
from .schemas import ShowSchemasPlugin
from .tables import ShowTablesPlugin

__all__ = [
    CreateAsPlugin,
    CreateTablePlugin,
    PredictModelPlugin,
    ShowColumnsPlugin,
    ShowSchemasPlugin,
    ShowTablesPlugin,
]
