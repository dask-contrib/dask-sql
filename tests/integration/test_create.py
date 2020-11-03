import pytest
import dask.dataframe as dd
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

    df = c.sql(
        """
        SELECT * FROM new_table
    """
    ).compute()

    assert_frame_equal(df, df)


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
