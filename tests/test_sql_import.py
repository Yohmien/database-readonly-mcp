import json
import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from sqlalchemy.engine import URL

from readonly_mcp.config import Connection, Rejected
from readonly_mcp.server import run_tool
from readonly_mcp.sql_import import (
    ObjectCounts, _group_inserts, build_jobs, database_name, drop_sql,
    run_sql_import, scan_dump, status_sql,
)


DUMP = """/*!40101 SET NAMES utf8mb4 */;

-- Dumping structure for table source.orders
CREATE TABLE IF NOT EXISTS `orders` (
  `id` int NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

-- Dumping data for table source.orders: ~3 rows (approximately)
DELETE FROM `orders`;
INSERT INTO `orders` (`id`) VALUES
\t(1),
\t(2);
INSERT INTO `orders` (`id`) VALUES
\t(3);

-- Dumping structure for view source.order_view
-- Creating temporary table to overcome VIEW dependency errors
CREATE TABLE `order_view` (`id` int);
DROP TABLE IF EXISTS `order_view`;
CREATE VIEW `order_view` AS SELECT id FROM orders;

-- Dumping structure for table source.dict
CREATE TABLE IF NOT EXISTS `dict` (`id` int) ENGINE=InnoDB;
-- Dumping data for table source.dict: ~1 row (approximately)
DELETE FROM `dict`;
INSERT INTO `dict` (`id`) VALUES (1);

SET FOREIGN_KEY_CHECKS=1;
"""


class DumpScanTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def make_dump(self, content=DUMP, name="2026_11_20-2.sql"):
        path = self.root / name
        path.write_text(content, encoding="utf-8", newline="\n")
        return path

    def test_database_name_is_exact_file_stem(self):
        path = self.make_dump()
        self.assertEqual(database_name(path), "2026_11_20-2")
        for bad in ("relative.sql", str(self.root / "a.b.sql"), str(self.root / "x.txt")):
            with self.subTest(bad=bad), self.assertRaises(Rejected):
                database_name(bad)

    def test_scan_separates_tables_data_suffix_and_auxiliary_objects(self):
        plan = scan_dump(self.make_dump())
        self.assertEqual([block.name for block in plan.table_blocks], ["orders", "dict"])
        self.assertEqual([block.name for block in plan.auxiliary_blocks], ["order_view"])
        self.assertEqual([len(block.inserts) for block in plan.table_blocks], [2, 1])
        self.assertIsNotNone(plan.prelude)
        self.assertEqual(plan.blocks[0].start, plan.prelude.end)
        jobs = build_jobs(plan, chunk_mb=1)
        self.assertEqual(len(jobs.setup), 3)
        self.assertEqual(len(jobs.data), 2)
        self.assertIsNotNone(jobs.finish)
        self.assertIsNone(jobs.auxiliary)
        self.assertTrue(all(
            job.label.startswith(("table:", "view:")) for job in jobs.setup
        ))

    def test_insert_ranges_end_after_multiline_values(self):
        plan = scan_dump(self.make_dump())
        orders = plan.table_blocks[0]
        first = orders.inserts[0]
        self.assertTrue(
            plan.path.read_bytes()[first.start:first.end].startswith(b"INSERT INTO `orders`")
        )
        self.assertTrue(
            plan.path.read_bytes()[first.start:first.end].rstrip().endswith(b"(2);")
        )

    def test_large_inserts_are_grouped_without_splitting_statements(self):
        class Block:
            inserts = [
                type("Range", (), {"start": 0, "end": 300_000})(),
                type("Range", (), {"start": 300_000, "end": 600_000})(),
                type("Range", (), {"start": 600_000, "end": 900_000})(),
                type("Range", (), {"start": 900_000, "end": 1_200_000})(),
            ]

        groups = _group_inserts(Block(), 1_000_000)
        self.assertEqual(
            [(group.start, group.end) for group in groups],
            [(0, 900_000), (900_000, 1_200_000)],
        )

    def test_sequential_mode_for_dump_without_structure_markers(self):
        plan = scan_dump(self.make_dump("CREATE TABLE t(id int);\nINSERT INTO t VALUES(1);\n"))
        jobs = build_jobs(plan)
        self.assertFalse(plan.parallel)
        self.assertEqual(len(jobs.setup), 1)
        self.assertEqual(jobs.setup[0].ranges[0].start, 0)
        self.assertEqual(jobs.setup[0].ranges[0].end, plan.size)


class FakeResult:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value

    def scalars(self):
        return self.value

    def all(self):
        return self.value


class FakeConn:
    def __init__(self):
        self.exists = False
        self.calls = []

    def execute(self, statement, params=None):
        sql = str(statement)
        if "information_schema.SCHEMATA" in sql:
            return FakeResult(int(self.exists))
        return FakeResult(0)

    def exec_driver_sql(self, sql, params=None):
        self.calls.append(sql)
        if sql.startswith("SELECT @@server_uuid"):
            return FakeResult("instance-uuid")
        if sql.startswith("CREATE DATABASE"):
            self.exists = True
        elif sql.startswith("DROP DATABASE"):
            self.exists = False
        return FakeResult(None)


class SqlImportLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.entry = Connection(
            "source",
            URL.create("mysql+pymysql", username="readonly", host="localhost", port=3306),
        )
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.sql = self.root / "new-db-1.sql"
        self.sql.write_text(DUMP, encoding="utf-8", newline="\n")
        self.state = self.root / "sql-import-state.json"
        self.env = patch.dict(os.environ, {"DB_MCP_SQL_IMPORT_STATE_FILE": str(self.state)})
        self.env.start()
        self.conn = FakeConn()

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    @contextmanager
    def admin(self, *args):
        yield self.conn

    @contextmanager
    def scoped(self, *args):
        yield "dbmuser", "secret"

    def arguments(self, **extra):
        return {"action": "import", "sql_path": str(self.sql), **extra}

    def test_import_runs_phases_and_keeps_database_for_explicit_drop(self):
        with patch("readonly_mcp.sql_import.admin_url", return_value=self.entry.url), patch(
            "readonly_mcp.sql_import.admin_session", self.admin,
        ), patch("readonly_mcp.sql_import.scoped_account", self.scoped), patch(
            "readonly_mcp.sql_import.cleanup_account",
        ), patch("readonly_mcp.sql_import.run_pipeline", return_value={
            "durationSeconds": 1, "phases": [{"phase": "data"}],
            "dataJobs": 2, "completedDataJobs": 2,
        }), patch(
            "readonly_mcp.sql_import._count_objects",
            return_value=ObjectCounts(tables=2, views=1, routines=0, triggers=0),
        ), patch("readonly_mcp.sql_import._assert_expected_objects"):
            result = run_sql_import(self.entry, self.arguments(verify_sql="SELECT 1"))
        self.assertEqual(result["database"], "new-db-1")
        self.assertEqual(result["mode"], "parallel")
        self.assertTrue(self.conn.exists)
        self.assertTrue(any(call.startswith("CREATE DATABASE") for call in self.conn.calls))
        self.assertFalse(any(call.startswith("DROP DATABASE") for call in self.conn.calls))
        with patch("readonly_mcp.sql_import.admin_url", return_value=self.entry.url), patch(
            "readonly_mcp.sql_import.admin_session", self.admin,
        ):
            status = status_sql(self.entry, {"sql_path": str(self.sql)})
        self.assertEqual(status["status"], "ready")

    def test_failed_import_drops_new_database_and_records_failure(self):
        with patch("readonly_mcp.sql_import.admin_url", return_value=self.entry.url), patch(
            "readonly_mcp.sql_import.admin_session", self.admin,
        ), patch("readonly_mcp.sql_import.scoped_account", self.scoped), patch(
            "readonly_mcp.sql_import.cleanup_account",
        ), patch("readonly_mcp.sql_import.run_pipeline", side_effect=Rejected("模拟导入失败")):
            with self.assertRaisesRegex(Rejected, "模拟导入失败"):
                run_sql_import(self.entry, self.arguments())
        self.assertFalse(self.conn.exists)
        self.assertTrue(any(call.startswith("DROP DATABASE") for call in self.conn.calls))
        state = json.loads(self.state.read_text(encoding="utf-8"))
        self.assertTrue(next(iter(state.values()))["deleted"])

    def test_existing_unregistered_database_is_never_adopted(self):
        self.conn.exists = True
        with patch("readonly_mcp.sql_import.admin_url", return_value=self.entry.url), patch(
            "readonly_mcp.sql_import.admin_session", self.admin,
        ), patch("readonly_mcp.sql_import.run_pipeline") as pipeline:
            with self.assertRaisesRegex(Rejected, "已存在"):
                run_sql_import(self.entry, self.arguments())
            with self.assertRaisesRegex(Rejected, "拒绝删除"):
                drop_sql(self.entry, {"action": "drop", "sql_path": str(self.sql)})
        pipeline.assert_not_called()

    def test_drop_only_removes_registered_database(self):
        with patch("readonly_mcp.sql_import.admin_url", return_value=self.entry.url), patch(
            "readonly_mcp.sql_import.admin_session", self.admin,
        ), patch("readonly_mcp.sql_import.scoped_account", self.scoped), patch(
            "readonly_mcp.sql_import.cleanup_account",
        ), patch("readonly_mcp.sql_import.run_pipeline", return_value={
            "durationSeconds": 1, "phases": [], "dataJobs": 1, "completedDataJobs": 1,
        }), patch(
            "readonly_mcp.sql_import._count_objects",
            return_value=ObjectCounts(tables=2, views=1, routines=0, triggers=0),
        ), patch("readonly_mcp.sql_import._assert_expected_objects"):
            run_sql_import(self.entry, self.arguments())
        with patch("readonly_mcp.sql_import.admin_url", return_value=self.entry.url), patch(
            "readonly_mcp.sql_import.admin_session", self.admin,
        ):
            result = drop_sql(self.entry, {"action": "drop", "sql_path": str(self.sql)})
        self.assertFalse(result["exists"])
        self.assertFalse(self.conn.exists)

    def test_drop_of_import_over_one_gb_requires_explicit_confirmation(self):
        with patch("readonly_mcp.sql_import.admin_url", return_value=self.entry.url), patch(
            "readonly_mcp.sql_import.admin_session", self.admin,
        ), patch("readonly_mcp.sql_import.scoped_account", self.scoped), patch(
            "readonly_mcp.sql_import.cleanup_account",
        ), patch("readonly_mcp.sql_import.run_pipeline", return_value={
            "durationSeconds": 1, "phases": [], "dataJobs": 1, "completedDataJobs": 1,
        }), patch(
            "readonly_mcp.sql_import._count_objects",
            return_value=ObjectCounts(tables=2, views=1, routines=0, triggers=0),
        ), patch("readonly_mcp.sql_import._assert_expected_objects"):
            run_sql_import(self.entry, self.arguments())
        with patch("readonly_mcp.sql_import.admin_url", return_value=self.entry.url), patch(
            "readonly_mcp.sql_import.admin_session", self.admin,
        ), patch("readonly_mcp.sql_import._database_size", return_value=2 * 1024 ** 3):
            with self.assertRaisesRegex(Rejected, "confirm_large_drop"):
                drop_sql(self.entry, {"action": "drop", "sql_path": str(self.sql)})
            self.assertTrue(self.conn.exists)
            result = drop_sql(self.entry, {
                "action": "drop", "sql_path": str(self.sql), "confirm_large_drop": True,
            })
        self.assertFalse(result["exists"])
        self.assertFalse(self.conn.exists)

    def test_drop_rejects_non_boolean_confirmation(self):
        with patch("readonly_mcp.sql_import.admin_url", return_value=self.entry.url), patch(
            "readonly_mcp.sql_import.admin_session", self.admin,
        ), patch("readonly_mcp.sql_import.scoped_account", self.scoped), patch(
            "readonly_mcp.sql_import.cleanup_account",
        ), patch("readonly_mcp.sql_import.run_pipeline", return_value={
            "durationSeconds": 1, "phases": [], "dataJobs": 1, "completedDataJobs": 1,
        }), patch(
            "readonly_mcp.sql_import._count_objects",
            return_value=ObjectCounts(tables=2, views=1, routines=0, triggers=0),
        ), patch("readonly_mcp.sql_import._assert_expected_objects"):
            run_sql_import(self.entry, self.arguments())
        with patch("readonly_mcp.sql_import.admin_url", return_value=self.entry.url), patch(
            "readonly_mcp.sql_import.admin_session", self.admin,
        ):
            with self.assertRaisesRegex(Rejected, "布尔值"):
                drop_sql(self.entry, {
                    "action": "drop", "sql_path": str(self.sql), "confirm_large_drop": "yes",
                })
        self.assertTrue(self.conn.exists)

    def test_database_name_can_override_file_stem(self):
        with patch("readonly_mcp.sql_import.admin_url", return_value=self.entry.url), patch(
            "readonly_mcp.sql_import.admin_session", self.admin,
        ), patch("readonly_mcp.sql_import.scoped_account", self.scoped), patch(
            "readonly_mcp.sql_import.cleanup_account",
        ), patch("readonly_mcp.sql_import.run_pipeline", return_value={
            "durationSeconds": 1, "phases": [], "dataJobs": 1, "completedDataJobs": 1,
        }), patch(
            "readonly_mcp.sql_import._count_objects",
            return_value=ObjectCounts(tables=2, views=1, routines=0, triggers=0),
        ), patch("readonly_mcp.sql_import._assert_expected_objects"):
            result = run_sql_import(self.entry, self.arguments(database_name="custom_db_1"))
        self.assertEqual(result["database"], "custom_db_1")
        state = json.loads(self.state.read_text(encoding="utf-8"))
        self.assertEqual(next(iter(state.values()))["database"], "custom_db_1")
        with patch("readonly_mcp.sql_import.admin_url", return_value=self.entry.url), patch(
            "readonly_mcp.sql_import.admin_session", self.admin,
        ):
            status = status_sql(self.entry, {"database_name": "custom_db_1"})
            self.assertEqual(status["status"], "ready")
            dropped = drop_sql(self.entry, {"action": "drop", "database_name": "custom_db_1"})
        self.assertFalse(dropped["exists"])
        self.assertFalse(self.conn.exists)

    def test_invalid_or_missing_database_name_is_rejected(self):
        with self.assertRaisesRegex(Rejected, "反引号"):
            drop_sql(self.entry, {"action": "drop", "database_name": "bad`name"})
        with self.assertRaisesRegex(Rejected, "sql_path 或 database_name"):
            drop_sql(self.entry, {"action": "drop"})

    def test_tool_routes_to_sql_import_and_rejects_relative_path(self):
        arguments = {"action": "import", "sql_path": "relative.sql"}
        with patch("readonly_mcp.server.run_sql_import", return_value={"status": "ok"}) as run:
            result = run_tool("db_sql_import", arguments, [self.entry])
        self.assertEqual(result.structuredContent, {"status": "ok"})
        run.assert_called_once()
        with self.assertRaises(Rejected):
            database_name("relative.sql")


if __name__ == "__main__":
    unittest.main()
