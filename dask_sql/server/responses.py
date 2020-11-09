from typing import List
import uuid

from fastapi import Request, FastAPI
import dask.dataframe as dd
import numpy as np

from dask_sql.mappings import python_to_sql_type


class StageStats:
    def __init__(self):
        self.stageId = ""
        self.state = ""
        self.done = True
        self.nodes = 0
        self.totalSplits = 0
        self.queuedSplits = 0
        self.runningSplits = 0
        self.completedSplits = 0
        self.cpuTimeMillis = 0
        self.wallTimeMillis = 0
        self.processedRows = 0
        self.processedBytes = 0
        self.subStages = []


class StatementStats:
    def __init__(self):
        self.state = ""
        self.queued = False
        self.scheduled = False
        self.nodes = 0
        self.totalSplits = 0
        self.queuedSplits = 0
        self.runningSplits = 0
        self.completedSplits = 0
        self.cpuTimeMillis = 0
        self.wallTimeMillis = 0
        self.queuedTimeMillis = 0
        self.elapsedTimeMillis = 0
        self.processedRows = 0
        self.processedBytes = 0
        self.peakMemoryBytes = 0
        self.peakTotalMemoryBytes = 0
        self.peakTaskTotalMemoryBytes = 0
        self.spilledBytes = 0
        self.rootStage = StageStats()


class QueryResults:
    def __init__(self, request: Request, next_url: str = None, cancel_url: str = None):
        empty_url = str(request.url.replace(path=request.app.url_path_for("empty")))

        self.id = str(uuid.uuid4())
        self.infoUri = empty_url
        if next_url:
            self.nextUri = next_url
        if cancel_url:
            self.partialCancelUri = cancel_url
        self.stats = StatementStats()
        self.warnings = []


class DataResults(QueryResults):
    @staticmethod
    def get_column_description(df):
        sql_types = [str(python_to_sql_type(t)) for t in df.dtypes]
        column_names = df.columns
        return [
            {
                "name": column_name,
                "type": sql_type.lower(),
                "typeSignature": {"rawType": sql_type.lower(), "arguments": []},
            }
            for column_name, sql_type in zip(column_names, sql_types)
        ]

    @staticmethod
    def get_data_description(df):
        return [
            DataResults.convert_row(row)
            for row in df.itertuples(index=False, name=None)
        ]

    @staticmethod
    def convert_cell(cell):
        try:
            if np.isnan(cell):  # pragma: no cover
                return "NaN"
            elif np.isposinf(cell):
                return "+Infinity"
            elif np.isneginf(cell):  # pragma: no cover
                return "-Infinity"
        except TypeError:  # pragma: no cover
            pass

        try:
            return cell.item()
        except AttributeError:
            pass

        return cell

    @staticmethod
    def convert_row(row):
        return [DataResults.convert_cell(cell) for cell in row]

    def __init__(self, df: dd.DataFrame, request: Request):
        super().__init__(request)

        if df is None:
            return

        self.columns = self.get_column_description(df)
        self.data = self.get_data_description(df)


class ErrorResults(QueryResults):
    def __init__(self, error: Exception, request: Request):
        super().__init__(request)

        self.error = QueryError(error)


class QueryError:
    def __init__(self, error: Exception):
        self.message = str(error)
        self.errorCode = 0
        self.errorName = str(type(error))
        self.errorType = "USER_ERROR"

        try:
            self.errorLocation = {
                "lineNumber": error.from_line + 1,
                "columnNumber": error.from_col + 1,
            }
        except AttributeError:  # pragma: no cover
            pass
