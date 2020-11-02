import pytest
from fastapi.testclient import TestClient

from dask_sql.server.app import app


@pytest.fixture()
def app_client(c):
    app.c = c
    yield TestClient(app)


def test_routes(app_client):
    assert app_client.post("/v1/statement", data="SELECT 1 + 1").status_code == 200
    assert app_client.get("/v1/statement", data="SELECT 1 + 1").status_code == 405
    assert app_client.get("/v1/empty").status_code == 200


def test_sql_query(app_client):
    response = app_client.post("/v1/statement", data="SELECT 1 + 1")
    assert response.status_code == 200

    result = response.json()

    assert "columns" in result
    assert "data" in result
    assert result["columns"] == [
        {
            "name": "1 + 1",
            "type": "integer",
            "typeSignature": {"rawType": "integer", "arguments": []},
        }
    ]

    assert result["data"] == [[2]]
    assert "error" not in result


def test_wrong_sql_query(app_client):
    response = app_client.post("/v1/statement", data="SELECT 1 + ")
    assert response.status_code == 200

    result = response.json()

    assert "columns" not in result
    assert "data" not in result
    assert "error" in result
    assert "message" in result["error"]
    assert "errorLocation" in result["error"]
    assert result["error"]["errorLocation"] == {"lineNumber": 1, "columnNumber": 10}


def test_add_and_query(app_client, df, temporary_data_file):
    df.to_csv(temporary_data_file, index=False)

    response = app_client.post(
        "/v1/statement",
        data=f"""
        CREATE TABLE
            new_table
        WITH (
            location = '{temporary_data_file}',
            format = 'csv'
        )
    """,
    )
    assert response.status_code == 200

    response = app_client.post("/v1/statement", data="SELECT * FROM new_table")
    assert response.status_code == 200

    result = response.json()

    assert "columns" in result
    assert "data" in result
    assert result["columns"] == [
        {
            "name": "a",
            "type": "double",
            "typeSignature": {"rawType": "double", "arguments": []},
        },
        {
            "name": "b",
            "type": "double",
            "typeSignature": {"rawType": "double", "arguments": []},
        },
    ]

    assert len(result["data"]) == 700
    assert "error" not in result
