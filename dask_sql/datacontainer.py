from typing import List, Dict, Tuple, Union

import dask.dataframe as dd


class ColumnContainer:
    pass


class ColumnContainer:
    def __init__(
        self,
        frontend_columns: List[str],
        frontend_backend_mapping: Union[Dict[str, str], None] = None,
    ):
        self._frontend_columns = list(frontend_columns)
        if frontend_backend_mapping is None:
            self._frontend_backend_mapping = {
                col: col for col in self._frontend_columns
            }
        else:
            self._frontend_backend_mapping = frontend_backend_mapping

    def _copy(self) -> ColumnContainer:
        return ColumnContainer(self._frontend_columns, self._frontend_backend_mapping)

    def limit_to(self, fields: List[str]) -> ColumnContainer:
        assert all(f in self._frontend_backend_mapping for f in fields)
        cc = self._copy()
        cc._frontend_columns = list(fields)
        return cc

    def rename(self, columns: Dict[str, str]) -> ColumnContainer:
        cc = self._copy()
        for column_from, column_to in columns.items():
            backend_column = self._frontend_backend_mapping[column_from]
            cc._frontend_backend_mapping[column_to] = backend_column

        cc._frontend_columns = [
            columns[col] if col in columns else col for col in self._frontend_columns
        ]

        return cc

    def mapping(self) -> List[Tuple[str, str]]:
        return self._frontend_backend_mapping.items()

    def reverse_mapping(self) -> List[Tuple[str, str]]:
        return {backend: frontend for frontend, backend in self.mapping()}

    @property
    def columns(self) -> List[str]:
        return self._frontend_columns

    def add(
        self, frontend_column: str, backend_column: Union[str, None] = None
    ) -> ColumnContainer:
        cc = self._copy()

        cc._frontend_backend_mapping[frontend_column] = (
            backend_column or frontend_column
        )
        cc._frontend_columns.append(frontend_column)

        return cc

    def get_backend_by_frontend_index(self, index: int) -> str:
        frontend_column = self._frontend_columns[index]
        backend_column = self._frontend_backend_mapping[frontend_column]
        return backend_column

    def make_unique(self, prefix="col"):
        """
        Make sure we have unique column names by calling each column

            prefix_number

        where number is the column index.
        """
        return self.rename(
            columns={col: f"{prefix}_{i}" for i, col in enumerate(self.columns)}
        )


class DataContainer:
    def __init__(self, df: dd.DataFrame, column_container: ColumnContainer):
        self.df = df
        self.column_container = column_container

    def assign(self) -> dd.DataFrame:
        df = self.df.rename(columns=self.column_container.reverse_mapping())
        return df[self.column_container.columns]
