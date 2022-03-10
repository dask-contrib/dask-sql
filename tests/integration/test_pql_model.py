import os
import pickle
import sys

import pandas as pd
import pytest
from dask.datasets import timeseries

from tests.integration.fixtures import skip_if_external_scheduler

# Create model statement escaping `target` and `timeseries` which are reserved words
create_model_sql = """
CREATE MODEL my_model (
    x number,
    y number,
    "target" number
)
TARGET "target"
AS (
    SELECT x, y, x*y > 0 AS "target"
    FROM "timeseries"
)
"""

# TODO: Remove LIMIT for now

create_model_join_tables_sql = """
CREATE MODEL my_model (
    x number,
    y number,
    "target" number
)
TARGET "target"
AS (
    SELECT t1.x, t2.y, t1.x*t2.y > 0 AS "target"
    FROM table1 as t1 
    INNER JOIN table2 as t2 on t1.id = t2.id
)
"""


def check_trained_model(c, model_name=None):
    if model_name is None:
        sql = """
        PREDICT "target"
        GIVEN SELECT x, y FROM "timeseries"
        """
    else:
        sql = """
        PREDICT "target" USING {}
        GIVEN SELECT x, y FROM "timeseries"
        """.format(model_name)

    tables_before = c.schema["root"].tables.keys()
    result_df = c.sql(sql).compute() # This will go ahead and run compute with this predict statement

    print("RESULT DF", result_df)

    # assert that there are no additional tables in context from prediction
    assert tables_before == c.schema["root"].tables.keys()
    assert "target" in result_df.columns
    assert len(result_df["target"]) > 0


@pytest.fixture()
def training_df(c):
    df = timeseries(freq="1d").reset_index(drop=True)
    c.create_table("timeseries", df, persist=True)

    return None


def test_training_and_prediction(c, training_df):
    c.sql(create_model_sql)

    check_trained_model(c, "my_model")


def test_show_models(c, training_df):
    c.sql(create_model_sql)

    expected = pd.DataFrame(["my_model"], columns=["Models"])
    result: pd.DataFrame = c.sql("SHOW MODELS").compute()
    # test
    pd.testing.assert_frame_equal(expected, result)


def test_drop_model(c, training_df):
    with pytest.raises(RuntimeError):
        c.sql("DROP MODEL my_model")

    c.sql("DROP MODEL IF EXISTS my_model")

    c.sql(create_model_sql)

    c.sql("DROP MODEL IF EXISTS my_model")

    assert "my_model" not in c.schema[c.schema_name].models