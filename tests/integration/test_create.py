import pytest
import dask.dataframe as dd
import pandas as pd
from pandas.testing import assert_frame_equal


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

    df = c.sql(
        """
        SELECT * FROM new_table
    """
    ).compute()

    assert_frame_equal(df, df)


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
        CREATE TABLE
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
        CREATE VIEW
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
