import pytest


def test_alter_schema(c):
    c.create_schema("test_schema")
    c.sql("ALTER SCHEMA test_schema RENAME TO prod_schema")
    assert "prod_schema" in c.schema

    with pytest.raises(RuntimeError):
        c.sql("ALTER SCHEMA MARVEL RENAME TO DC")

    del c.schema["prod_schema"]


def test_alter_table(c, df_simple):
    c.create_table("maths", df_simple)
    c.sql("ALTER TABLE maths RENAME TO physics")
    assert "physics" in c.schema[c.schema_name].tables

    with pytest.raises(RuntimeError):
        c.sql("ALTER TABLE four_legs RENAME TO two_legs")

    c.sql("ALTER TABLE IF EXISTS alien RENAME TO humans")

    print(c.schema[c.schema_name].tables)
    del c.schema[c.schema_name].tables["physics"]
