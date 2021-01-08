from typing import Any

try:
    import intake
except ImportError:  # pragma: no cover
    intake = None

from dask_sql.input_utils.base import BaseInputPlugin


class IntakeCatalogInputPlugin(BaseInputPlugin):
    """Input Plugin for Intake Catalogs, getting the table in dask format"""

    def is_correct_input(
        self, input_item: Any, table_name: str, format: str = None, **kwargs
    ):
        return intake and (
            isinstance(input_item, intake.catalog.Catalog) or format == "intake"
        )

    def to_dc(self, input_item: Any, table_name: str, format: str = None, **kwargs):
        table_name = kwargs.pop("intake_table_name", table_name)
        catalog_kwargs = kwargs.pop("catalog_kwargs", {})

        if isinstance(input_item, str):
            input_item = intake.open_catalog(input_item, **catalog_kwargs)

        return input_item[table_name].to_dask(**kwargs)
