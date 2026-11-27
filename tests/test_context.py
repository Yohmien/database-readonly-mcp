import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from jsonschema import Draft202012Validator
from sqlalchemy.engine import URL

from readonly_mcp.backend import Backend
from readonly_mcp.config import Connection, Rejected
from readonly_mcp.server import TOOLS, run_tool


class ResponseTests(unittest.TestCase):
    def setUp(self):
        self.entry = Connection("test", URL.create("mysql+pymysql", host="localhost", database="fixture"))
        self.result = {"connection": "test", "database": "fixture", "columns": ["id", "name"],
                       "rows": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
                       "values": [[1, "a"], [2, "b"]], "count": 2, "truncated": False, "elapsedMs": 1}

    def call(self, name="db_query", **options):
        with patch.object(Backend, "query", return_value=self.result):
            return run_tool(name, {"sql": "SELECT 1", **options}, [self.entry])

    def test_object_array_and_compatibility_formats(self):
        objects = self.call().structuredContent
        self.assertEqual(objects["rows"], self.result["rows"])
        self.assertNotIn("values", objects)
        arrays = self.call(row_format="arrays").structuredContent
        self.assertEqual(arrays["values"], self.result["values"])
        self.assertNotIn("rows", arrays)
        both = self.call(row_format="both")
        self.assertEqual(both.structuredContent, self.result)
        self.assertEqual(json.loads(both.content[0].text), both.structuredContent)
        self.assertEqual(self.result["values"], [[1, "a"], [2, "b"]])

    def test_duplicate_columns_keep_every_value(self):
        self.result.update(columns=["name", "name"], rows=[{"name": "b"}], values=[["a", "b"]], count=1)
        for format in ("objects", "arrays", "both"):
            with self.subTest(format=format):
                self.assertEqual(self.call(row_format=format).structuredContent["values"], [["a", "b"]])

    def test_removed_query_tool_is_rejected_before_database(self):
        with patch.object(Backend, "query") as query, self.assertRaises(Rejected):
            run_tool("mysql_query", {"sql": "SELECT 1"}, [self.entry])
        query.assert_not_called()

    def test_bad_format_rejected_before_database(self):
        with patch.object(Backend, "query") as query, self.assertRaises(Rejected):
            run_tool("db_query", {"sql": "SELECT 1", "row_format": "unknown"}, [self.entry])
        query.assert_not_called()

    def test_query_default_and_explicit_limits(self):
        with patch.object(Backend, "query", return_value=self.result) as query:
            run_tool("db_query", {"sql": "SELECT 1"}, [self.entry])
            run_tool("db_query", {"sql": "SELECT 1", "limit": 250}, [self.entry])
        limits = [call.args[2] for call in query.call_args_list]
        self.assertEqual(limits, [100, 250])

    def test_backend_default_cap_follows_connection(self):
        entry = Connection("small", URL.create("sqlite", database=str(Path(self.entry.url.database or ":memory:"))), max_rows=7)
        self.assertIsNotNone(entry.max_rows)

    def test_schemas_remain_self_contained_and_valid(self):
        for name, _, properties, required in TOOLS:
            with self.subTest(tool=name):
                schema = {"type": "object", "properties": properties, "required": required, "additionalProperties": False}
                Draft202012Validator.check_schema(schema)
                self.assertNotIn("$ref", json.dumps(schema))


class DirectTaskTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "fixture.db"
        with closing(sqlite3.connect(self.path)) as conn:
            conn.executescript("CREATE TABLE item(id INTEGER PRIMARY KEY,name TEXT UNIQUE);")
            conn.executemany("INSERT INTO item VALUES (?,?)", [(n, str(n)) for n in range(1, 202)])
            conn.commit()
        self.entry = Connection("test", URL.create("sqlite", database=str(self.path)), max_rows=1000)
        (self.path.parent / "application.properties").write_text("spring.datasource.url=jdbc:sqlite:" + str(self.path).replace("\\", "/") + "\n", encoding="utf-8")

    def tearDown(self):
        self.temp.cleanup()

    def test_default_limit_and_explicit_expansion(self):
        routing = {"project": str(self.path.parent), "sql": "SELECT * FROM item ORDER BY id"}
        result = run_tool("db_query", routing, [self.entry]).structuredContent
        self.assertEqual(result["count"], 100)
        self.assertTrue(result["truncated"])
        expanded = run_tool("db_query", {**routing, "limit": 201}, [self.entry]).structuredContent
        self.assertEqual(expanded["count"], 201)
        self.assertFalse(expanded["truncated"])
        with self.assertRaises(Rejected):
            run_tool("db_query", {**routing, "limit": 1001}, [self.entry])

    def test_describe_columns_or_full_without_preflight(self):
        request = {"project": str(self.path.parent), "table": "item"}
        columns = run_tool("db_describe", request, [self.entry]).structuredContent
        self.assertEqual([column["name"] for column in columns["columns"]], ["id", "name"])
        self.assertNotIn("indexes", columns)
        full = run_tool("db_describe", {**request, "detail": "full"}, [self.entry]).structuredContent
        self.assertEqual(full["primaryKey"]["constrained_columns"], ["id"])
        self.assertEqual(full["columns"], columns["columns"])
        self.assertEqual(full["uniqueConstraints"][0]["column_names"], ["name"])

if __name__ == "__main__":
    unittest.main()
