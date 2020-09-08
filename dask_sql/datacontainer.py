from typing import List, Dict, Tuple, Union

import dask.dataframe as dd


ColumnType = Union[str, int]


class ColumnContainer:
    pass


class ColumnContainer:
    def __init__(
        self,
        frontend_columns: List[str],
        frontend_backend_mapping: Union[Dict[str, ColumnType], None] = None,
    ):
        assert all(
            isinstance(col, str) for col in frontend_columns
        ), "All frontend columns need to be of string type"
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
        cc._frontend_columns = [str(x) for x in fields]
        return cc

    def rename(self, columns: Dict[str, ColumnType]) -> ColumnContainer:
        cc = self._copy()
        for column_from, column_to in columns.items():
            backend_column = self._frontend_backend_mapping[str(column_from)]
            cc._frontend_backend_mapping[str(column_to)] = backend_column

        cc._frontend_columns = [
            str(columns[col]) if col in columns else col
            for col in self._frontend_columns
        ]

        return cc

    def mapping(self) -> List[Tuple[str, str]]:
        return list(self._frontend_backend_mapping.items())

    @property
    def columns(self) -> List[str]:
        return self._frontend_columns

    def add(
        self, frontend_column: str, backend_column: Union[str, None] = None
    ) -> ColumnContainer:
        cc = self._copy()

        frontend_column = str(frontend_column)

        cc._frontend_backend_mapping[frontend_column] = str(
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
            columns={str(col): f"{prefix}_{i}" for i, col in enumerate(self.columns)}
        )


class DataContainer:
    def __init__(self, df: dd.DataFrame, column_container: ColumnContainer):
        self.df = df
        self.column_container = column_container

    def assign(self) -> dd.DataFrame:
        df = self.df.assign(
            **{
                col_from: self.df[col_to]
                for col_from, col_to in self.column_container.mapping()
            }
        )
        return df[self.column_container.columns]
