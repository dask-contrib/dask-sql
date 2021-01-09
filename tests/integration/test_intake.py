import shutil
import tempfile
import os

import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from dask_sql.context import Context

# skip the test if intake is not installed
intake = pytest.importorskip("intake")


@pytest.fixture()
def intake_catalog_location():
    tmpdir = tempfile.mkdtemp()

    df = pd.DataFrame({"a": [1], "b": [1.5]})

    csv_location = os.path.join(tmpdir, "data.csv")
    df.to_csv(csv_location, index=False)

    yaml_location = os.path.join(tmpdir, "catalog.yaml")
    with open(yaml_location, "w") as f:
        f.write(
            """sources:
    intake_table:
        args:
            urlpath: "{{ CATALOG_DIR }}/data.csv"
        description: "Some Data"
        driver: csv
        """
        )

    try:
        yield yaml_location
    finally:
        shutil.rmtree(tmpdir)


def check_read_table(c):
    result_df = c.sql("SELECT * FROM df").compute().reset_index(drop=True)
    df = pd.DataFrame({"a": [1], "b": [1.5]})

    assert_frame_equal(df, result_df)


def test_intake_catalog(intake_catalog_location):
    catalog = intake.open_catalog(intake_catalog_location)
    c = Context()
    c.create_table("df", catalog, intake_table_name="intake_table")

    check_read_table(c)


def test_intake_location(intake_catalog_location):
    c = Context()
    c.create_table(
        "df", intake_catalog_location, format="intake", intake_table_name="intake_table"
    )

    check_read_table(c)


def test_intake_sql(intake_catalog_location):
    c = Context()
    c.sql(
        f"""
        CREATE TABLE df WITH (
         location = '{intake_catalog_location}',
         format = 'intake',
         intake_table_name = 'intake_table'
        )
    """
    )

    check_read_table(c)
