import pytest

import pandas as pd
from pandas.testing import assert_frame_equal

fugue_sql = pytest.importorskip("fugue_sql")
# needs to be imported after the check for fugue
from dask_sql.integrations.fugue import DaskSQLExecutionEngine


def test_simple_statement():
    with fugue_sql.FugueSQLWorkflow(DaskSQLExecutionEngine) as dag:
        # Define a data frame and use it
        df = dag.df([[0, "hello"], [1, "world"]], "a:int64,b:str")
        dag(
            """
        result = SELECT * FROM df WHERE a > 0
        """
        )

        return_df = dag["result"].compute().as_pandas()
        assert_frame_equal(return_df, pd.DataFrame({"a": [1], "b": ["world"]}))

