import shutil
import tempfile
import pytest
import time

import pandas as pd
from pandas.testing import assert_frame_equal

from dask_sql.context import Context

# skip the test if the docker or sqlalchemy package is not installed
docker = pytest.importorskip("docker")
sqlalchemy = pytest.importorskip("sqlalchemy")

pytest.importorskip("pyhive")


DEFAULT_CONFIG = {
    "HIVE_SITE_CONF_javax_jdo_option_ConnectionURL": "jdbc:postgresql://hive-metastore-postgresql/metastore",
    "HIVE_SITE_CONF_javax_jdo_option_ConnectionDriverName": "org.postgresql.Driver",
    "HIVE_SITE_CONF_javax_jdo_option_ConnectionUserName": "hive",
    "HIVE_SITE_CONF_javax_jdo_option_ConnectionPassword": "hive",
    "HIVE_SITE_CONF_datanucleus_autoCreateSchema": "false",
    "HIVE_SITE_CONF_hive_metastore_uris": "thrift://hive-metastore:9083",
    "HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check": "false",
    "CORE_CONF_fs_defaultFS": "file:///database",
    "CORE_CONF_hadoop_http_staticuser_user": "root",
    "CORE_CONF_hadoop_proxyuser_hue_hosts": "*",
    "CORE_CONF_hadoop_proxyuser_hue_groups": "*",
    "HIVE_SITE_CONF_fs_default_name": "file:///database",
    "CORE_CONF_fs_defaultFS": "file:///database",
    "HIVE_SIZE_CONF_hive_metastore_warehouse_dir": "file:///database",
}


@pytest.fixture(scope="session")
def hive_cursor():
    """
    Getting a hive setup up and running is a bit more complicated.
    We need three running docker containers:
    * a postgres database to store the metadata
    * the metadata server itself
    * and a server to answer SQL queries

    They are all started one after the other to check,
    if they are up and running.
    We "fake" a network filesystem (instead of using hdfs),
    by mounting a temporary folder from the host to the
    docker container, which can be accessed both by hive
    and the dask-sql client.

    We just need to make sure, to remove all containers,
    the network and the temporary folders correctly again.

    The ideas for the docker setup are taken from the docker-compose
    hive setup described by bde2020.
    """
    client = docker.from_env()

    network = None
    hive_server = None
    hive_metastore = None
    hive_postgres = None

    tmpdir = tempfile.mkdtemp()
    tmpdir_parted = tempfile.mkdtemp()

    try:
        network = client.networks.create("dask-sql-hive", driver="bridge")

        hive_server = client.containers.create(
            "bde2020/hive:2.3.2-postgresql-metastore",
            hostname="hive-server",
            name="hive-server",
            network="dask-sql-hive",
            volumes=[f"{tmpdir}:{tmpdir}", f"{tmpdir_parted}:{tmpdir_parted}"],
            environment={
                "HIVE_CORE_CONF_javax_jdo_option_ConnectionURL": "jdbc:postgresql://hive-metastore-postgresql/metastore",
                **DEFAULT_CONFIG,
            },
        )

        hive_metastore = client.containers.create(
            "bde2020/hive:2.3.2-postgresql-metastore",
            hostname="hive-metastore",
            name="hive-metastore",
            network="dask-sql-hive",
            environment=DEFAULT_CONFIG,
            command="/opt/hive/bin/hive --service metastore",
        )

        hive_postgres = client.containers.create(
            "bde2020/hive-metastore-postgresql:2.3.0",
            hostname="hive-metastore-postgresql",
            name="hive-metastore-postgresql",
            network="dask-sql-hive",
        )

        # Wait for it to start
        hive_postgres.start()
        hive_postgres.exec_run(["bash"])
        for l in hive_postgres.logs(stream=True):
            if b"ready for start up." in l:
                break

        hive_metastore.start()
        hive_metastore.exec_run(["bash"])
        for l in hive_metastore.logs(stream=True):
            if b"Starting hive metastore" in l:
                break

        hive_server.start()
        hive_server.exec_run(["bash"])
        for l in hive_server.logs(stream=True):
            if b"Starting HiveServer2" in l:
                break

        # The server needs some time to start.
        # It is easier to check for the first access
        # on the metastore than to wait some
        # arbitrary time.
        for l in hive_metastore.logs(stream=True):
            if b"get_multi_table" in l:
                break

        time.sleep(2)

        hive_server.reload()
        address = hive_server.attrs["NetworkSettings"]["Networks"]["dask-sql-hive"][
            "IPAddress"
        ]
        port = 10000
        cursor = sqlalchemy.create_engine(f"hive://{address}:{port}").connect()

        # Create a non-partitioned column
        cursor.execute(
            f"CREATE TABLE df (i INTEGER, j INTEGER) ROW FORMAT DELIMITED STORED AS PARQUET LOCATION '{tmpdir}'"
        )
        cursor.execute("INSERT INTO df (i, j) VALUES (1, 2)")
        cursor.execute("INSERT INTO df (i, j) VALUES (2, 4)")

        cursor.execute(
            f"CREATE TABLE df_part (i INTEGER) PARTITIONED BY (j INTEGER) ROW FORMAT DELIMITED STORED AS PARQUET LOCATION '{tmpdir_parted}'"
        )
        cursor.execute("INSERT INTO df_part PARTITION (j=2) (i) VALUES (1)")
        cursor.execute("INSERT INTO df_part PARTITION (j=4) (i) VALUES (2)")

        # The data files are created as root user by default. Change that:
        hive_server.exec_run(["chmod", "a+rwx", "-R", tmpdir])
        hive_server.exec_run(["chmod", "a+rwx", "-R", tmpdir_parted])

        yield cursor
    finally:
        # Now clean up: remove the containers and the network and the folders
        for container in [hive_server, hive_metastore, hive_postgres]:
            if container is None:
                continue

            try:
                container.kill()
            except:
                pass

            container.remove()

        if network is not None:
            network.remove()

        shutil.rmtree(tmpdir)
        shutil.rmtree(tmpdir_parted)


def test_select(hive_cursor):
    c = Context()
    c.create_table("df", hive_cursor)

    result_df = c.sql("SELECT * FROM df").compute().reset_index(drop=True)
    df = pd.DataFrame({"i": [1, 2], "j": [2, 4]}).astype("int32")

    assert_frame_equal(df, result_df)


def test_select_partitions(hive_cursor):
    c = Context()
    c.create_table("df_part", hive_cursor)

    result_df = c.sql("SELECT * FROM df_part").compute().reset_index(drop=True)
    df = pd.DataFrame({"i": [1, 2], "j": [2, 4]}).astype("int32")
    df["j"] = df["j"].astype("int64")

    assert_frame_equal(df, result_df)
