from .analyze import AnalyzeTablePlugin
from .columns import ShowColumnsPlugin
from .create_experiment import CreateExperimentPlugin
from .create_model import CreateModelPlugin
from .create_schema import CreateSchemaPlugin
from .create_table import CreateTablePlugin
from .create_table_as import CreateTableAsPlugin
from .describe_model import ShowModelParamsPlugin
from .drop_model import DropModelPlugin
from .drop_schema import DropSchemaPlugin
from .drop_table import DropTablePlugin
from .export_model import ExportModelPlugin
from .predict import PredictModelPlugin
from .schemas import ShowSchemasPlugin
from .show_models import ShowModelsPlugin
from .switch_schema import SwitchSchemaPlugin
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
    ShowModelsPlugin,
    ShowModelParamsPlugin,
    ExportModelPlugin,
    CreateExperimentPlugin,
    CreateSchemaPlugin,
    SwitchSchemaPlugin,
    DropSchemaPlugin,
]
