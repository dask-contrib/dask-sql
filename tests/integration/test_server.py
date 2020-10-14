import os
import tempfile

from fastapi.testclient import TestClient

from dask_sql import Context
from dask_sql.server.app import app
from tests.integration.fixtures import DaskTestCase


class TestServer(DaskTestCase):
    def setUp(self):
        super().setUp()

        app.c = Context()
        self.client = TestClient(app)

        self.f = os.path.join(tempfile.gettempdir(), os.urandom(24).hex())

    def tearDown(self):
        super().tearDown()

        if os.path.exists(self.f):
            os.unlink(self.f)

    def test_routes(self):
        self.assertEqual(
            self.client.post("/v1/statement", data="SELECT 1 + 1").status_code, 200
        )
        self.assertEqual(
            self.client.get("/v1/statement", data="SELECT 1 + 1").status_code, 405
        )
        self.assertEqual(self.client.get("/v1/empty").status_code, 200)

    def test_sql_query(self):
        response = self.client.post("/v1/statement", data="SELECT 1 + 1")
        self.assertEqual(response.status_code, 200)

        result = response.json()

        self.assertIn("columns", result)
        self.assertIn("data", result)
        self.assertEqual(
            result["columns"],
            [
                {
                    "name": "1 + 1",
                    "type": "integer",
                    "typeSignature": {"rawType": "integer", "arguments": []},
                }
            ],
        )
        self.assertEqual(result["data"], [[2]])
        self.assertNotIn("error", result)

    def test_wrong_sql_query(self):
        response = self.client.post("/v1/statement", data="SELECT 1 + ")
        self.assertEqual(response.status_code, 200)

        result = response.json()

        self.assertNotIn("columns", result)
        self.assertNotIn("data", result)
        self.assertIn("error", result)
        self.assertIn("message", result["error"])
        self.assertIn("errorLocation", result["error"])
        self.assertEqual(
            result["error"]["errorLocation"], {"lineNumber": 1, "columnNumber": 10}
        )

    def test_add_and_query(self):
        self.df.to_csv(self.f, index=False)

        response = self.client.post(
            "/v1/statement",
            data=f"""
            CREATE TABLE
                new_table
            WITH (
                location = '{self.f}',
                format = 'csv'
            )
        """,
        )
        self.assertEqual(response.status_code, 200)

        response = self.client.post("/v1/statement", data="SELECT * FROM new_table")
        self.assertEqual(response.status_code, 200)

        result = response.json()

        self.assertIn("columns", result)
        self.assertIn("data", result)
        self.assertEqual(
            result["columns"],
            [
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
            ],
        )
        self.assertEqual(len(result["data"]), 700)
        self.assertNotIn("error", result)
