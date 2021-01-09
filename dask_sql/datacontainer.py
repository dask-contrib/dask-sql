from typing import List, Dict, Tuple, Union

import dask.dataframe as dd


ColumnType = Union[str, int]


class ColumnContainer:
    # Forward declaration
    pass


class ColumnContainer:
    """
    Helper class to store a list of columns,
    which do not necessarily be the ones of the dask dataframe.
    Instead, the container also stores a mapping from "frontend"
    columns (columns with the names and order expected by SQL)
    to "backend" columns (the real column names used by dask)
    to prevent unnecessary renames.
    """

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
        """
        Internal function to copy this container
        """
        return ColumnContainer(self._frontend_columns, self._frontend_backend_mapping)

    def limit_to(self, fields: List[str]) -> ColumnContainer:
        """
        Create a new ColumnContainer, which has frontend columns
        limited to only the ones given as parameter.
        Also uses the order of these as the new column order.
        """
        if not fields:
            return self  # pragma: no cover

        assert all(f in self._frontend_backend_mapping for f in fields)
        cc = self._copy()
        cc._frontend_columns = [str(x) for x in fields]
        return cc

    def rename(self, columns: Dict[str, str]) -> ColumnContainer:
        """
        Return a new ColumnContainer where the frontend columns
        are renamed according to the given mapping.
        Columns not present in the mapping are not touched,
        the order is preserved.
        """
        cc = self._copy()
        for column_from, column_to in columns.items():
            backend_column = self._frontend_backend_mapping[str(column_from)]
            cc._frontend_backend_mapping[str(column_to)] = backend_column

        cc._frontend_columns = [
            str(columns[col]) if col in columns else col
            for col in self._frontend_columns
        ]

        return cc

    def mapping(self) -> List[Tuple[str, ColumnType]]:
        """
        The mapping from frontend columns to backend columns.
        """
        return list(self._frontend_backend_mapping.items())

    @property
    def columns(self) -> List[str]:
        """
        The stored frontend columns in the correct order
        """
        return self._frontend_columns

    def add(
        self, frontend_column: str, backend_column: Union[str, None] = None
    ) -> ColumnContainer:
        """
        Return a new ColumnContainer with the
        given column added.
        The column is added at the last position in the column list.
        """
        cc = self._copy()

        frontend_column = str(frontend_column)

        cc._frontend_backend_mapping[frontend_column] = str(
            backend_column or frontend_column
        )
        cc._frontend_columns.append(frontend_column)

        return cc

    def get_backend_by_frontend_index(self, index: int) -> str:
        """
        Get back the dask column, which is referenced by the
        frontend (SQL) column with the given index.
        """
        frontend_column = self._frontend_columns[index]
        backend_column = self._frontend_backend_mapping[frontend_column]
        return backend_column

    def get_backend_by_frontend_name(self, column: str) -> str:
        """
        Get back the dask column, which is referenced by the
        frontend (SQL) column with the given name.
        """
        backend_column = self._frontend_backend_mapping[column]
        return backend_column

    def make_unique(self, prefix="col"):
        """
        Make sure we have unique column names by calling each column

            <prefix>_<number>

        where <number> is the column index.
        """
        return self.rename(
            columns={str(col): f"{prefix}_{i}" for i, col in enumerate(self.columns)}
        )


class DataContainer:
    """
    In SQL, every column operation or reference is done via
    the column index. Some dask operations, such as grouping,
    joining or concatenating preserve the columns in a different
    order than SQL would expect.
    However, we do not want to change the column data itself
    all the time (because this would lead to computational overhead),
    but still would like to keep the columns accessible by name and index.
    For this, we add an additional `ColumnContainer` to each dataframe,
    which does all the column mapping between "frontend"
    (what SQL expects, also in the correct order)
    and "backend" (what dask has).
    """

    def __init__(self, df: dd.DataFrame, column_container: ColumnContainer):
        self.df = df
        self.column_container = column_container

    def assign(self) -> dd.DataFrame:
        """
        Combine the column mapping with the actual data and return
        a dataframe which has the the columns specified in the
        stored ColumnContainer.
        """
        df = self.df.assign(
            **{
                col_from: self.df[col_to]
                for col_from, col_to in self.column_container.mapping()
                if col_from in self.column_container.columns
            }
        )
        return df[self.column_container.columns]
