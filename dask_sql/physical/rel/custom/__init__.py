from .analyze import AnalyzeTablePlugin
from .columns import ShowColumnsPlugin
from .create_model import CreateModelPlugin
from .create_table import CreateTablePlugin
from .create_table_as import CreateTableAsPlugin
from .drop_model import DropModelPlugin
from .drop_table import DropTablePlugin
from .predict import PredictModelPlugin
from .schemas import ShowSchemasPlugin
from .tables import ShowTablesPlugin

__all__ = [
    AnalyzeTablePlugin,
    CreateModelPlugin,
    CreateTableAsPlugin,
    CreateTablePlugin,
    DropModelPlugin,
    DropTablePlugin,
    PredictModelPlugin,
    ShowColumnsPlugin,
    ShowSchemasPlugin,
    ShowTablesPlugin,
]
