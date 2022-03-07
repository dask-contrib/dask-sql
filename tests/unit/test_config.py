import os
from unittest import mock

import pytest
import yaml
from dask import config as dask_config

# Required to instantiate default sql config
import dask_sql  # noqa: F401


def test_custom_yaml(tmpdir):
    custom_config = {}
    custom_config["sql"] = dask_config.get("sql")
    custom_config["sql"]["groupby"]["split_out"] = 16
    custom_config["sql"]["foo"] = {"bar": [1, 2, 3], "baz": None}

    with open(os.path.join(tmpdir, "custom-sql.yaml"), mode="w") as f:
        yaml.dump(custom_config, f)

    dask_config.refresh(
        paths=[tmpdir]
    )  # Refresh config to read from updated environment
    assert custom_config["sql"] == dask_config.get("sql")
    dask_config.refresh()


def test_env_variable():
    with mock.patch.dict("os.environ", {"DASK_SQL__GROUPBY__SPLIT_OUT": "200"}):
        dask_config.refresh()
        assert dask_config.get("sql.groupby.split-out") == 200
    dask_config.refresh()


def test_default_config():
    config_fn = os.path.join(os.path.dirname(__file__), "../../dask_sql", "sql.yaml")
    with open(config_fn) as f:
        default_config = yaml.safe_load(f)
    assert "sql" in default_config
    assert default_config["sql"] == dask_config.get("sql")


def test_schema():
    jsonschema = pytest.importorskip("jsonschema")

    config_fn = os.path.join(os.path.dirname(__file__), "../../dask_sql", "sql.yaml")
    schema_fn = os.path.join(
        os.path.dirname(__file__), "../../dask_sql", "sql-schema.yaml"
    )

    with open(config_fn) as f:
        config = yaml.safe_load(f)

    with open(schema_fn) as f:
        schema = yaml.safe_load(f)

    jsonschema.validate(config, schema)


def test_schema_is_complete():
    config_fn = os.path.join(os.path.dirname(__file__), "../../dask_sql", "sql.yaml")
    schema_fn = os.path.join(
        os.path.dirname(__file__), "../../dask_sql", "sql-schema.yaml"
    )

    with open(config_fn) as f:
        config = yaml.safe_load(f)

    with open(schema_fn) as f:
        schema = yaml.safe_load(f)

    def test_matches(c, s):
        for k, v in c.items():
            if list(c) != list(s["properties"]):
                raise ValueError(
                    "\nThe sql.yaml and sql-schema.yaml files are not in sync.\n"
                    "This usually happens when we add a new configuration value,\n"
                    "but don't add the schema of that value to the dask-schema.yaml file\n"
                    "Please modify these files to include the missing values: \n\n"
                    "    sql.yaml:        {}\n"
                    "    sql-schema.yaml: {}\n\n"
                    "Examples in these files should be a good start, \n"
                    "even if you are not familiar with the jsonschema spec".format(
                        sorted(c), sorted(s["properties"])
                    )
                )
            if isinstance(v, dict):
                test_matches(c[k], s["properties"][k])

    test_matches(config, schema)


def test_dask_setconfig():
    dask_config.set({"sql.foo.bar": 1})
    with dask_config.set({"sql.foo.baz": "2"}):
        assert dask_config.get("sql.foo") == {"bar": 1, "baz": "2"}
    assert dask_config.get("sql.foo") == {"bar": 1}
    dask_config.refresh()
