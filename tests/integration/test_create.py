import pytest
import dask.dataframe as dd
import pandas as pd
from pandas.testing import assert_frame_equal

import dask_sql


def test_create_from_csv(c, df, temporary_data_file):
    df.to_csv(temporary_data_file, index=False)

    c.sql(
        f"""
        CREATE TABLE
            new_table
        WITH (
            location = '{temporary_data_file}',
            format = 'csv'
        )
    """
    )

    result_df = c.sql(
        """
        SELECT * FROM new_table
    """
    ).compute()

    assert_frame_equal(result_df, df)


def test_cluster_memory(client, c, df):
    client.publish_dataset(df=dd.from_pandas(df, npartitions=1))

    c.sql(
        f"""
        CREATE TABLE
            new_table
        WITH (
            location = 'df',
            format = 'memory'
        )
    """
    )

    return_df = c.sql(
        """
        SELECT * FROM new_table
    """
    ).compute()

    assert_frame_equal(df, return_df)


def test_create_from_csv_persist(c, df, temporary_data_file):
    df.to_csv(temporary_data_file, index=False)

    c.sql(
        f"""
        CREATE TABLE
            new_table
        WITH (
            location = '{temporary_data_file}',
            format = 'csv',
            persist = True
        )
    """
    )

    return_df = c.sql(
        """
        SELECT * FROM new_table
    """
    ).compute()

    assert_frame_equal(df, return_df)


def test_wrong_create(c):
    with pytest.raises(AttributeError):
        c.sql(
            f"""
            CREATE TABLE
                new_table
            WITH (
                format = 'csv'
            )
        """
        )

    with pytest.raises(AttributeError):
        c.sql(
            f"""
            CREATE TABLE
                new_table
            WITH (
                format = 'strange',
                location = 'some/path'
            )
        """
        )


def test_create_from_query(c, df):
    c.sql(
        f"""
        CREATE OR REPLACE TABLE
            new_table
        AS (
            SELECT * FROM df
        )
    """
    )

    return_df = c.sql(
        """
        SELECT * FROM new_table
    """
    ).compute()

    assert_frame_equal(df, return_df)

    c.sql(
        f"""
        CREATE OR REPLACE VIEW
            new_table
        AS (
            SELECT * FROM df
        )
    """
    )

    return_df = c.sql(
        """
        SELECT * FROM new_table
    """
    ).compute()

    assert_frame_equal(df, return_df)


def test_view_table_persist(c, temporary_data_file, df):
    df.to_csv(temporary_data_file, index=False)
    c.sql(
        f"""
        CREATE TABLE
            new_table
        WITH (
            location = '{temporary_data_file}',
            format = 'csv'
        )
    """
    )

    # Views should change, when the original data changes
    # Tables should not change, when the original data changes
    c.sql(
        f"""
        CREATE VIEW
            count_view
        AS (
            SELECT COUNT(*) AS c FROM new_table
        )
    """
    )
    c.sql(
        f"""
        CREATE TABLE
            count_table
        AS (
            SELECT COUNT(*) AS c FROM new_table
        )
    """
    )

    assert_frame_equal(
        c.sql("SELECT c FROM count_view").compute(), pd.DataFrame({"c": [700]})
    )
    assert_frame_equal(
        c.sql("SELECT c FROM count_table").compute(), pd.DataFrame({"c": [700]})
    )

    df.iloc[:10].to_csv(temporary_data_file, index=False)

    assert_frame_equal(
        c.sql("SELECT c FROM count_view").compute(), pd.DataFrame({"c": [10]})
    )
    assert_frame_equal(
        c.sql("SELECT c FROM count_table").compute(), pd.DataFrame({"c": [700]})
    )


def test_replace_and_error(c, temporary_data_file, df):
    c.sql(
        f"""
        CREATE TABLE
            new_table
        AS (
            SELECT 1 AS a
        )
    """
    )

    assert_frame_equal(
        c.sql("SELECT a FROM new_table").compute(),
        pd.DataFrame({"a": [1]}),
        check_dtype=False,
    )

    with pytest.raises(RuntimeError):
        c.sql(
            f"""
            CREATE TABLE
                new_table
            AS (
                SELECT 1
            )
        """
        )

    c.sql(
        f"""
        CREATE TABLE IF NOT EXISTS
            new_table
        AS (
            SELECT 2 AS a
        )
    """
    )

    assert_frame_equal(
        c.sql("SELECT a FROM new_table").compute(),
        pd.DataFrame({"a": [1]}),
        check_dtype=False,
    )

    c.sql(
        f"""
        CREATE OR REPLACE TABLE
            new_table
        AS (
            SELECT 2 AS a
        )
    """
    )

    assert_frame_equal(
        c.sql("SELECT a FROM new_table").compute(),
        pd.DataFrame({"a": [2]}),
        check_dtype=False,
    )

    c.sql("DROP TABLE new_table")

    with pytest.raises(dask_sql.utils.ParsingException):
        c.sql("SELECT a FROM new_table")

    c.sql(
        f"""
        CREATE TABLE IF NOT EXISTS
            new_table
        AS (
            SELECT 3 AS a
        )
    """
    )

    assert_frame_equal(
        c.sql("SELECT a FROM new_table").compute(),
        pd.DataFrame({"a": [3]}),
        check_dtype=False,
    )

    df.to_csv(temporary_data_file, index=False)
    with pytest.raises(RuntimeError):
        c.sql(
            f"""
            CREATE TABLE
                new_table
            WITH (
                location = '{temporary_data_file}',
                format = 'csv'
            )
        """
        )

    c.sql(
        f"""
        CREATE TABLE IF NOT EXISTS
            new_table
        WITH (
            location = '{temporary_data_file}',
            format = 'csv'
        )
    """
    )

    assert_frame_equal(
        c.sql("SELECT a FROM new_table").compute(),
        pd.DataFrame({"a": [3]}),
        check_dtype=False,
    )

    c.sql(
        f"""
        CREATE OR REPLACE TABLE
            new_table
        WITH (
            location = '{temporary_data_file}',
            format = 'csv'
        )
    """
    )

    result_df = c.sql(
        """
        SELECT * FROM new_table
    """
    ).compute()

    assert_frame_equal(result_df, df)


def test_drop(c):
    with pytest.raises(RuntimeError):
        c.sql("DROP TABLE new_table")

    c.sql("DROP TABLE IF EXISTS new_table")

    c.sql(
        f"""
        CREATE TABLE
            new_table
        AS (
            SELECT 1 AS a
        )
    """
    )

    c.sql("DROP TABLE IF EXISTS new_table")

    with pytest.raises(dask_sql.utils.ParsingException):
        c.sql("SELECT a FROM new_table")
