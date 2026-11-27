import json
import os
import sys
import tempfile
import unittest
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from sqlalchemy.engine import URL

from readonly_mcp.config import Connection, Rejected
from readonly_mcp.mirror import (dataset_dump, dataset_selection, ensure_local_infile, execute_scoped,
                                export_test_dataset, extend_test_dataset, identity, load_test_dataset, mysqlsh_call,
                                named_mirror, prepare_test_dataset, resolved_identity, run_client, run_mirror,
                                verify_writer)
from readonly_mcp.server import run_tool


class StaticTests(unittest.TestCase):
    def check(self, sql):
        return run_tool("db_sql_check", {"sql": sql, "dialect": "mysql"}, []).structuredContent

    def test_offline_valid_invalid_and_unsupported(self):
        self.assertEqual(self.check("CREATE TABLE t(id INT); SELECT * FROM t;")["status"], "passed")
        self.assertEqual(self.check("SELECT (1")["status"], "failed")
        self.assertEqual(self.check("garbage")["status"], "incomplete")
        self.assertEqual(self.check("PREPARE s FROM @sql")["status"], "incomplete")

    def test_high_risk_is_visible_without_executing(self):
        self.assertTrue(self.check("DELETE FROM t WHERE id=1")["risks"])
        self.assertTrue(self.check("UPDATE t SET n=2")["risks"])
        self.assertFalse(self.check("SELECT * FROM t")["risks"])

    def test_mysql_client_script_routes_to_real_mirror_execution(self):
        for sql in (
            "DELIMITER $$\\nCREATE PROCEDURE p() SELECT 1$$\\nDELIMITER ;",
            "CREATE FUNCTION f() RETURNS INT RETURN 1;",
            "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW SET NEW.id=1;",
        ):
            with self.subTest(sql=sql):
                result = self.check(sql)
                self.assertEqual(result["status"], "incomplete")
                self.assertEqual(result["recommendedTool"], "db_script_test")


class MirrorTests(unittest.TestCase):
    def setUp(self):
        self.entry = Connection("source", URL.create("mysql+pymysql", username="readonly", host="localhost", database="warehouse"))
        self.temp = tempfile.TemporaryDirectory()
        self.state = Path(self.temp.name) / "state.json"
        self.env = patch.dict(os.environ, {"DB_MCP_MIRROR_STATE_FILE": str(self.state)})
        self.env.start()
        self.identity_patch = patch("readonly_mcp.mirror.resolved_identity", return_value=identity(self.entry, "test-instance", "warehouse"))
        self.identity_patch.start()
        self.exists = False
        self.calls = []

    def tearDown(self):
        self.env.stop()
        self.identity_patch.stop()
        self.temp.cleanup()

    def call(self, action, **args):
        return run_tool("db_mirror", {"action": action, "connection": "source", **args}, [self.entry]).structuredContent

    def script_test(self, **args):
        return run_tool("db_script_test", {
            "connection": "source",
            "prepare": "empty",
            "sql": "DELIMITER $$\\nCREATE PROCEDURE p() SELECT 1$$\\nDELIMITER ;",
            "verify_sql": "SELECT COUNT(*) AS n FROM item",
            **args,
        }, [self.entry]).structuredContent

    def dataset_args(self, **args):
        return {
            "connection": "source",
            "action": "prepare",
            "tables": ["orders", "dict"],
            "prune": {"orders": "out_order_no LIKE 'DEMO-001%'"},
            "supplement_tables": ["dict"],
            **args,
        }

    def test_alias_independent_source_identity(self):
        self.assertEqual(identity(self.entry), identity(replace(self.entry, name="another-alias")))
        other = replace(self.entry, url=self.entry.url.set(database="another"))
        self.assertNotEqual(identity(self.entry)[0], identity(other)[0])
        self.assertRegex(identity(self.entry)[1], r"^dbm[0-9a-f]{32}$")
        address_alias = replace(self.entry, url=self.entry.url.set(host="127.0.0.1"))
        self.assertEqual(identity(self.entry, "same-server", "warehouse"), identity(address_alias, "same-server", "warehouse"))

    def test_named_mirror_requires_gate_and_never_matches_source(self):
        with self.assertRaises(Rejected):
            named_mirror(self.entry, "test-instance", "warehouse", "wms_it")
        with patch.dict(os.environ, {"DB_MCP_MIRROR_ALLOW_NAMED": "1"}):
            source_id, database, source = named_mirror(self.entry, "test-instance", "warehouse", "wms_it")
            self.assertEqual(database, "wms_it")
            self.assertNotEqual(database, source["database"])
            self.assertNotEqual(named_mirror(self.entry, "test-instance", "warehouse", "wms_it")[0],
                                identity(self.entry, "test-instance", "warehouse")[0])
            for bad in ("Warehouse", "wms-it", "wms.it", "dbm%", "warehouse"):
                with self.subTest(bad=bad), self.assertRaises(Rejected):
                    named_mirror(self.entry, "test-instance", "warehouse", bad)

    def test_writer_exact_scope_and_global_or_role_rejection(self):
        db = identity(self.entry)[1]
        verify_writer([(f"GRANT SELECT, CREATE, DROP ON `{db}`.* TO `w`@`localhost`",)], db)
        for scope in ("*.*", "`warehouse`.*", "`dbm%`.*", "`other`.*"):
            with self.subTest(scope=scope), self.assertRaises(Rejected):
                verify_writer([(f"GRANT ALL PRIVILEGES ON {scope} TO `w`@`localhost`",)], db)
        for grant in ("GRANT `role`@`%` TO `w`@`localhost`", f"GRANT SELECT ON `{db}`.* TO `w`@`localhost` WITH GRANT OPTION"):
            with self.assertRaises(Rejected):
                verify_writer([(grant,)], db)

    def test_missing_writer_is_explicit_no_database_call(self):
        with patch("readonly_mcp.mirror.admin_url", return_value=None), patch("readonly_mcp.mirror.create_engine") as engine:
            result = self.call("status")
            self.assertIsNone(result["exists"])
            self.assertFalse(result["configured"])
            with self.assertRaises(Rejected):
                self.call("create")
            engine.assert_not_called()

    @contextmanager
    def session(self, *args):
        test = self

        class Rows:
            def scalar(self):
                return int(test.exists)

            def scalars(self):
                return ["item"]

        class Conn:
            def execute(self, *args):
                return Rows()

            def exec_driver_sql(self, sql, *params):
                test.calls.append(sql)
                if sql.startswith("CREATE DATABASE"):
                    test.exists = True
                elif sql.startswith("DROP DATABASE"):
                    test.exists = False
        yield Conn()

    def test_lifecycle_ownership_repeat_execute_reset_and_cleanup(self):
        with patch("readonly_mcp.mirror.admin_url", return_value=self.entry.url), patch("readonly_mcp.mirror.admin_session", self.session), patch("readonly_mcp.mirror.source_defaults", return_value=("utf8mb4", "utf8mb4_general_ci")), patch("readonly_mcp.mirror.execute_scoped", return_value={"message": "ok"}) as execute, patch("readonly_mcp.mirror.source_tables", return_value=["item"]), patch("readonly_mcp.mirror.source_dump"):
            self.assertTrue(self.call("create")["exists"])
            self.call("create")
            self.assertEqual(sum(c.startswith("CREATE DATABASE") for c in self.calls), 1)
            self.call("execute", sql="INSERT INTO item VALUES(1);")
            self.call("clear")
            self.assertIn("TRUNCATE TABLE", execute.call_args.args[3])
            self.call("init_empty")
            self.call("clone")
            self.assertFalse(self.call("drop")["exists"])
            self.assertFalse(self.call("status")["exists"])
            tombstone = next(iter(json.loads(self.state.read_text()).values()))
            self.assertTrue(tombstone["deleted"])
            self.assertNotIn("account", tombstone)
            self.call("drop")

    def test_unregistered_existing_database_is_never_adopted(self):
        self.exists = True
        with patch("readonly_mcp.mirror.admin_url", return_value=self.entry.url), patch("readonly_mcp.mirror.admin_session", self.session):
            for action in ("create", "clear", "drop", "init_empty", "clone", "execute"):
                with self.subTest(action=action), self.assertRaises(Rejected):
                    self.call(action, **({"sql": "SELECT 1"} if action == "execute" else {}))
            self.assertEqual(self.calls, [])

    def test_failed_import_keeps_ownership_for_mcp_cleanup(self):
        with patch("readonly_mcp.mirror.admin_url", return_value=self.entry.url), patch("readonly_mcp.mirror.admin_session", self.session), patch("readonly_mcp.mirror.source_defaults", return_value=("utf8mb4", "utf8mb4_general_ci")), patch("readonly_mcp.mirror.source_tables", return_value=["item"]), patch("readonly_mcp.mirror.source_dump"), patch("readonly_mcp.mirror.execute_scoped", side_effect=Rejected("import failed")):
            with self.assertRaises(Rejected):
                self.call("init_empty")
            self.assertTrue(self.exists)
            self.assertTrue(self.state.exists())
            self.assertFalse(self.call("drop")["exists"])

    def test_invalid_parameters_rejected_before_engine(self):
        for action, options in (("create", {"sql": "SELECT 1"}), ("execute", {}), ("clear", {"tables": ["x"]}), ("status", {"timeout_seconds": 0})):
            with self.subTest(action=action), patch("readonly_mcp.mirror.create_engine") as engine, self.assertRaises(Rejected):
                self.call(action, **options)
            engine.assert_not_called()

    def test_mandatory_roles_rejected_before_caller_sql(self):
        class Row:
            def __init__(self, value):
                self.value = value

            def scalar(self):
                return self.value

        class Conn:
            def exec_driver_sql(self, sql):
                return Row("8.0.45" if sql == "SELECT VERSION()" else "mandatory_role")

        with patch("readonly_mcp.mirror.execute") as execute, self.assertRaises(Rejected):
            execute_scoped(Conn(), self.entry.url, "mirror", "SET ROLE ALL; DELETE FROM source.t;", 60, {}, "id", self.state)
        execute.assert_not_called()

    def test_parallel_clone_requires_explicit_tables_before_connecting(self):
        with patch("readonly_mcp.mirror.admin_url") as admin, self.assertRaises(Rejected):
            self.call("clone", clone_engine="mysqlsh")
        admin.assert_not_called()

    def test_local_infile_is_persisted_only_when_disabled(self):
        class Row:
            def __init__(self, value):
                self.value = value

            def scalar(self):
                return self.value

        class Conn:
            values = [0, 1]

            def __init__(self):
                self.calls = []

            def exec_driver_sql(self, sql):
                self.calls.append(sql)
                return Row(self.values.pop(0) if sql.startswith("SELECT") else None)

        conn = Conn()
        ensure_local_infile(conn)
        self.assertEqual(conn.calls, [
            "SELECT @@GLOBAL.local_infile",
            "SET PERSIST local_infile=ON",
            "SELECT @@GLOBAL.local_infile",
        ])

        enabled = Conn()
        enabled.values = [1]
        ensure_local_infile(enabled)
        self.assertEqual(enabled.calls, ["SELECT @@GLOBAL.local_infile"])

    def test_dataset_append_returns_failed_verification_for_next_round(self):
        source_id, _, source = identity(self.entry, "test-instance", "warehouse")
        self.exists = True
        self.state.write_text(json.dumps({source_id: {"source": source}}), encoding="utf-8")
        dump_dir = Path(self.temp.name) / "append-dump"
        dump_dir.mkdir()
        load_file = dump_dir / "load.sql"
        load_file.write_text("SELECT 1;\n", encoding="utf-8")
        with patch("readonly_mcp.mirror.admin_url", return_value=self.entry.url), patch(
                "readonly_mcp.mirror.admin_session", self.session,
        ), patch("readonly_mcp.mirror.source_tables", return_value=["item"]), patch(
                "readonly_mcp.mirror.execute_scoped",
                side_effect=[{"message": "载入成功"}, Rejected("隔离验证未通过")],
        ):
            result = run_mirror(self.entry, {
                "action": "dataset_append",
                "tables": ["item"],
                "dump_dir": str(dump_dir),
                "dataset_engine": "mysqldump",
                "load_file": str(load_file),
                "verify_sql": "SELECT 1;",
            })
        self.assertTrue(result["datasetAppend"])
        self.assertEqual(result["verificationStatus"], "failed")
        self.assertEqual(result["verificationError"], "隔离验证未通过")

    def test_script_test_runs_execute_and_verify_twice_then_drops(self):
        actions = []

        def mirror(config, arguments):
            action = arguments["action"]
            actions.append((action, arguments.get("sql")))
            if action == "status":
                return {"configured": True, "exists": False, "sourceId": "id", "source": {"database": "warehouse"},
                        "database": "dbm-test"}
            if action == "empty":
                return {"configured": True, "exists": True, "sourceId": "id", "source": {"database": "warehouse"},
                        "database": "dbm-test"}
            if action == "execute":
                return {"output": "1\\n" if arguments["sql"].startswith("SELECT") else "procedure ok\\n",
                        "truncated": False, "message": "ok"}
            if action == "drop":
                return {"exists": False}
            raise AssertionError(action)

        with patch("readonly_mcp.mirror.run_mirror", side_effect=mirror):
            result = self.script_test()
        self.assertEqual(result["status"], "passed")
        self.assertTrue(result["outputMatches"])
        self.assertTrue(result["verificationMatches"])
        self.assertFalse(result["cleanup"]["exists"])
        self.assertEqual([action for action, _ in actions],
                         ["status", "empty", "execute", "execute", "execute", "execute", "drop"])

    def test_script_test_failure_still_stops_and_cleans_mirror(self):
        script_runs = 0

        def mirror(config, arguments):
            nonlocal script_runs
            action = arguments["action"]
            if action == "status":
                return {"configured": True, "exists": False, "sourceId": "id", "source": {"database": "warehouse"},
                        "database": "dbm-test"}
            if action == "empty":
                return {"exists": True}
            if action == "execute" and not arguments["sql"].startswith("SELECT"):
                script_runs += 1
                if script_runs == 2:
                    raise Rejected("MySQL 客户端失败，错误码 1304")
                return {"output": "procedure ok\\n", "truncated": False}
            if action == "execute":
                return {"output": "1\\n", "truncated": False}
            if action == "drop":
                return {"exists": False}
            raise AssertionError(action)

        with patch("readonly_mcp.mirror.run_mirror", side_effect=mirror):
            result = self.script_test()
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["failure"], {
            "stage": "execute", "attempt": 2, "error": "MySQL 客户端失败，错误码 1304",
        })
        self.assertFalse(result["cleanup"]["exists"])

    def test_script_test_refuses_existing_mirror(self):
        with patch("readonly_mcp.mirror.run_mirror", return_value={
            "configured": True, "exists": True,
        }) as run:
            with self.assertRaises(Rejected):
                self.script_test()
        self.assertEqual(run.call_count, 1)

    def test_dataset_requires_partitioned_nonempty_prune(self):
        with patch("readonly_mcp.mirror.source_tables", return_value=["orders", "dict"]):
            selected, filters, supplements = dataset_selection(self.entry, self.dataset_args())
        self.assertEqual(selected, ["dict", "orders"])
        self.assertEqual(filters, {"warehouse.orders": "out_order_no LIKE 'DEMO-001%'"})
        self.assertEqual(supplements, ["dict"])

        invalid = [
            self.dataset_args(prune={}),
            self.dataset_args(supplement_tables=["orders"]),
            self.dataset_args(supplement_tables=[]),
            self.dataset_args(prune={"orders": "1=1; DROP TABLE source.orders"}),
            self.dataset_args(prune={"orders": "1=1 -- bypass"}),
        ]
        for arguments in invalid:
            with self.subTest(arguments=arguments), patch(
                    "readonly_mcp.mirror.source_tables", return_value=["orders", "dict"],
            ), self.assertRaises(Rejected):
                dataset_selection(self.entry, arguments)

    def test_schema_all_adds_empty_filters_without_repeated_exports(self):
        def tables(config, selected):
            return ["orders", "dict"] if selected is not None else ["orders", "dict", "audit_log"]

        with patch("readonly_mcp.mirror.source_tables", side_effect=tables):
            selected, filters, supplements = dataset_selection(
                self.entry, self.dataset_args(schema_mode="all"),
            )
        self.assertEqual(selected, ["audit_log", "dict", "orders"])
        self.assertEqual(filters["warehouse.audit_log"], "1=0")
        self.assertEqual(filters["warehouse.orders"], "out_order_no LIKE 'DEMO-001%'")
        self.assertEqual(supplements, ["dict"])

    def test_prepare_dataset_loads_pruned_dump_then_supplements_and_verifies(self):
        actions = []

        def mirror(config, arguments):
            actions.append(arguments)
            if arguments["action"] == "status":
                return {"configured": True, "exists": False}
            if arguments["action"] == "dataset_load":
                return {"configured": True, "exists": True, "database": "dbm-test", "tables": arguments["tables"]}
            raise AssertionError(arguments)

        with patch("readonly_mcp.mirror.source_tables", return_value=["orders", "dict"]), patch(
                "readonly_mcp.mirror.dataset_dump", return_value={"bytes": 1024, "files": 3, "engine": "mysqlsh"},
        ) as dump, patch("readonly_mcp.mirror.run_mirror", side_effect=mirror):
            result = prepare_test_dataset(self.entry, self.dataset_args(
                supplement_sql="INSERT IGNORE INTO dict VALUES (1);",
                verify_sql="SELECT COUNT(*) AS n FROM orders",
            ))
        self.assertEqual(result["action"], "prepare")
        self.assertEqual(result["prune"], {"orders": "out_order_no LIKE 'DEMO-001%'"})
        self.assertEqual(result["supplementTables"], ["dict"])
        self.assertEqual(dump.call_args.args[2]["warehouse.orders"], "out_order_no LIKE 'DEMO-001%'")
        load = next(action for action in actions if action["action"] == "dataset_load")
        self.assertIn("supplement_sql", load)
        self.assertIn("verify_sql", load)

    def test_export_dataset_writes_manifest_and_supplement_sql(self):
        output = Path(self.temp.name) / "dataset"

        def dump(config, selected, filters, threads, bytes_per_chunk, timeout, shell_home, dump_dir):
            self.assertTrue(dump_dir.parent.is_dir())
            dump_dir.mkdir()
            (dump_dir / "@.json").write_text("{}", encoding="utf-8")
            (dump_dir / "orders@1.sql").write_text("SELECT 1;\\n", encoding="utf-8")
            return {"bytes": 10, "files": 2, "engine": "mysqlsh"}

        with patch("readonly_mcp.mirror.source_tables", return_value=["orders", "dict"]), patch(
                "readonly_mcp.mirror.dataset_dump", side_effect=dump,
        ):
            result = export_test_dataset(self.entry, self.dataset_args(
                action="export", output_dir=str(output), supplement_sql="INSERT IGNORE INTO dict VALUES (1);",
            ))
        self.assertEqual(result["outputDir"], str(output))
        manifest = json.loads((output / "mcp-dataset.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["prune"], {"orders": "out_order_no LIKE 'DEMO-001%'"})
        self.assertTrue((output / "dump/orders@1.sql").is_file())
        self.assertTrue((output / "supplement.sql").is_file())
        with self.assertRaises(Rejected):
            with patch("readonly_mcp.mirror.source_tables", return_value=["orders", "dict"]):
                export_test_dataset(self.entry, self.dataset_args(action="export", output_dir=str(output)))

    def test_load_dataset_restores_export_manifest_and_supplement(self):
        bundle = Path(self.temp.name) / "bundle"
        dump = bundle / "dump"
        dump.mkdir(parents=True)
        extension_dump = bundle / "extensions/001/dump"
        extension_dump.mkdir(parents=True)
        (dump / "orders@1.sql").write_text("SELECT 1;\\n", encoding="utf-8")
        (extension_dump / "extra@1.sql").write_text("SELECT 1;\\n", encoding="utf-8")
        (bundle / "supplement.sql").write_text("INSERT IGNORE INTO dict VALUES (1);\\n", encoding="utf-8")
        (bundle / "mcp-dataset.json").write_text(json.dumps({
            "format": 1,
            "sourceDatabase": "warehouse",
            "tables": ["orders", "dict"],
            "prune": {"orders": "out_order_no LIKE 'DEMO-001%'"},
            "supplementTables": ["dict"],
            "dumpDir": "dump",
            "supplementSqlFile": "supplement.sql",
            "verifySql": "SELECT COUNT(*) AS n FROM orders",
            "threads": 6,
            "extensions": [{
                "path": "extensions/001",
                "tables": ["extra"],
                "prune": {"extra": "task_no = '1'"},
                "supplementTables": [],
                "dumpDir": "dump",
                "datasetEngine": "mysqlsh",
            }],
        }), encoding="utf-8")
        calls = []

        def mirror(config, arguments):
            calls.append(arguments)
            if arguments["action"] == "status":
                return {"configured": True, "exists": False}
            return {"configured": True, "exists": True, "database": "dbm-test"}

        with patch("readonly_mcp.mirror.source_tables", return_value=["orders", "dict"]), patch(
                "readonly_mcp.mirror.run_mirror", side_effect=mirror,
        ):
            result = load_test_dataset(self.entry, {"input_dir": str(bundle)})
        self.assertEqual(result["action"], "load")
        load = next(call for call in calls if call["action"] == "dataset_load")
        self.assertEqual(load["threads"], 6)
        self.assertIn("INSERT IGNORE", load["supplement_sql"])
        self.assertEqual(load["verify_sql"], "SELECT COUNT(*) AS n FROM orders")
        self.assertTrue(any(call["action"] == "dataset_append" for call in calls))

    def test_extend_supports_repeated_gap_repair_rounds(self):
        bundle = Path(self.temp.name) / "extend-bundle"
        (bundle / "dump").mkdir(parents=True)
        (bundle / "mcp-dataset.json").write_text(json.dumps({
            "format": 1,
            "sourceDatabase": "warehouse",
            "tables": ["orders", "dict"],
            "prune": {"orders": "out_order_no = '1'"},
            "supplementTables": ["dict"],
            "schemaMode": "all",
            "dumpDir": "dump",
            "datasetEngine": "mysqlsh",
            "threads": 6,
        }), encoding="utf-8")

        def dump(config, selected, filters, threads, bytes_per_chunk, timeout, shell_home, dump_dir):
            dump_dir.mkdir(parents=True)
            (dump_dir / "@.json").write_text("{}", encoding="utf-8")
            return {"bytes": 10, "files": 1, "engine": "mysqlsh", "fallbackReason": None}

        append_results = iter([
            {
                "configured": True,
                "exists": True,
                "datasetAppend": True,
                "verificationStatus": "failed",
                "verificationError": "隔离验证未通过",
            },
            {
                "configured": True,
                "exists": True,
                "datasetAppend": True,
                "verificationStatus": "passed",
            },
        ])

        def mirror(config, arguments):
            if arguments["action"] == "status":
                return {"configured": True, "exists": True}
            if arguments["action"] == "dataset_append":
                return next(append_results)
            raise AssertionError(arguments)

        with patch("readonly_mcp.mirror.source_tables", return_value=["missing"]), patch(
                "readonly_mcp.mirror.dataset_dump", side_effect=dump,
        ), patch("readonly_mcp.mirror.run_mirror", side_effect=mirror):
            first = extend_test_dataset(self.entry, {
                "input_dir": str(bundle),
                "tables": ["missing"],
                "prune": {"missing": "order_no = '1'"},
            })
            second = extend_test_dataset(self.entry, {
                "input_dir": str(bundle),
                "tables": ["missing"],
                "prune": {"missing": "order_no = '1'"},
            })
        self.assertEqual(first["extensionId"], "001")
        self.assertEqual(first["extensionStatus"], "failed")
        self.assertEqual(first["verificationStatus"], "failed")
        self.assertEqual(second["extensionId"], "002")
        self.assertEqual(second["extensionStatus"], "passed")
        self.assertEqual(second["verificationStatus"], "passed")
        manifest = json.loads((bundle / "mcp-dataset.json").read_text(encoding="utf-8"))
        self.assertEqual([item["id"] for item in manifest["extensions"]], ["001", "002"])
        self.assertEqual(
            [item["status"] for item in manifest["extensions"]],
            ["failed", "passed"],
        )
        self.assertTrue((bundle / "extensions/001/extension.json").is_file())
        self.assertTrue((bundle / "extensions/002/extension.json").is_file())

    def test_extend_records_load_failure_before_returning_error(self):
        bundle = Path(self.temp.name) / "failed-extend-bundle"
        (bundle / "dump").mkdir(parents=True)
        (bundle / "mcp-dataset.json").write_text(json.dumps({
            "format": 1,
            "sourceDatabase": "warehouse",
            "tables": ["orders"],
            "prune": {"orders": "out_order_no = '1'"},
            "supplementTables": [],
            "schemaMode": "selected",
            "dumpDir": "dump",
            "datasetEngine": "mysqlsh",
        }), encoding="utf-8")

        def dump(config, selected, filters, threads, bytes_per_chunk, timeout, shell_home, dump_dir):
            dump_dir.mkdir(parents=True)
            (dump_dir / "@.json").write_text("{}", encoding="utf-8")
            return {"bytes": 10, "files": 1, "engine": "mysqlsh", "fallbackReason": None}

        def mirror(config, arguments):
            if arguments["action"] == "status":
                return {"configured": True, "exists": True}
            if arguments["action"] == "dataset_append":
                raise Rejected("模拟增量载入失败")
            raise AssertionError(arguments)

        with patch("readonly_mcp.mirror.source_tables", return_value=["missing"]), patch(
                "readonly_mcp.mirror.dataset_dump", side_effect=dump,
        ), patch("readonly_mcp.mirror.run_mirror", side_effect=mirror):
            with self.assertRaisesRegex(Rejected, "增量轮次 001 已记录"):
                extend_test_dataset(self.entry, {
                    "input_dir": str(bundle),
                    "tables": ["missing"],
                    "prune": {"missing": "order_no = '1'"},
                })

        manifest = json.loads((bundle / "mcp-dataset.json").read_text(encoding="utf-8"))
        extension = manifest["extensions"][0]
        self.assertEqual(extension["id"], "001")
        self.assertEqual(extension["status"], "failed")
        self.assertEqual(extension["verificationStatus"], "not_run")
        self.assertIn("模拟增量载入失败", extension["failure"])
        self.assertTrue((bundle / "extensions/001/extension.json").is_file())

    def test_dataset_dump_falls_back_to_parallel_mysqldump(self):
        dump_dir = Path(self.temp.name) / "fallback-dump"
        shell_home = Path(self.temp.name) / "fallback-shell"
        shell_home.mkdir()

        def fallback(config, selected, filters, threads, timeout, target):
            target.mkdir(parents=True, exist_ok=True)
            (target / "orders.sql").write_text("SELECT 1;\\n", encoding="utf-8")

        with patch("readonly_mcp.mirror.mysqlsh_binary", return_value="mysqlsh"), patch(
                "readonly_mcp.mirror.mysqlsh_call", side_effect=Rejected("MYSQLSH 6001: path failure"),
        ), patch("readonly_mcp.mirror.dataset_dump_mysqldump", side_effect=fallback):
            result = dataset_dump(
                self.entry, ["orders"], {"warehouse.orders": "out_order_no = '1'"},
                8, "8M", 60, shell_home, dump_dir,
            )
        self.assertEqual(result["engine"], "mysqldump")
        self.assertIn("MYSQLSH 6001", result["fallbackReason"])
        self.assertTrue((dump_dir / "orders.sql").is_file())

    def test_mysqlsh_environment_keeps_windows_drive_and_valid_paths(self):
        with patch("readonly_mcp.mirror.subprocess.run") as run:
            run.return_value.returncode = 0
            mysqlsh_call("mysqlsh", self.entry.url, "dump_tables", [], 10, Path(self.temp.name))
        env = run.call_args.kwargs["env"]
        self.assertEqual(env["SYSTEMDRIVE"], os.environ["SYSTEMDRIVE"])
        self.assertTrue(all(Path(entry).is_dir() for entry in env["PATH"].split(os.pathsep) if entry))

    def test_client_login_file_cannot_override_mcp_credentials(self):
        with patch.dict(os.environ, {"MYSQL_TEST_LOGIN_FILE": "untrusted-login.cnf"}), patch("readonly_mcp.mirror.subprocess.run") as run:
            run.return_value.returncode = 0
            run_client(["mysql", "--no-defaults"], self.entry.url, 10, "SELECT 1")
            login = run.call_args.kwargs["env"]["MYSQL_TEST_LOGIN_FILE"]
            self.assertNotEqual(login, "untrusted-login.cnf")
            self.assertFalse(Path(login).exists())

    def test_dump_handoff_preserves_bytes_and_checks_size_before_restore(self):
        export = [sys.executable, "-c", "import sys; sys.stdout.buffer.write(bytes([0, 128, 255]))"]
        restore = [sys.executable, "-c", "import sys; print(sys.stdin.buffer.read().hex())"]
        with tempfile.TemporaryFile() as dump:
            self.assertEqual(run_client(export, self.entry.url, 10, output_limit=2, dump_output=dump),
                             ("", True))
        with tempfile.TemporaryFile() as dump:
            self.assertEqual(run_client(export, self.entry.url, 10, output_limit=3, dump_output=dump),
                             ("", False))
            output, truncated = run_client(restore, self.entry.url, 10, dump)
            self.assertEqual(output.strip(), "0080ff")
            self.assertFalse(truncated)

    def test_missing_source_recovers_registered_canonical_identity(self):
        class Row:
            def __init__(self, value):
                self.value = value

            def scalar(self):
                return self.value

        class Conn:
            def exec_driver_sql(self, sql):
                return Row("instance-uuid" if sql == "SELECT @@server_uuid" else 1)

            def execute(self, *args):
                return Row(None)

        canonical = identity(self.entry, "instance-uuid", "warehouse")
        state = {canonical[0]: {"source": canonical[2]}}
        configured = replace(self.entry, url=self.entry.url.set(database="Warehouse"))
        self.assertEqual(resolved_identity(Conn(), configured, state), canonical)
        with self.assertRaises(Rejected):
            resolved_identity(Conn(), configured, {})

    def test_missing_source_drop_then_status_and_repeated_drop(self):
        test = self
        canonical = identity(self.entry, "instance-uuid", "warehouse")
        self.state.write_text(json.dumps({canonical[0]: {"source": canonical[2]}}), encoding="utf-8")
        self.exists = True

        class Row:
            def __init__(self, value):
                self.value = value

            def scalar(self):
                return self.value

        class Conn:
            def exec_driver_sql(self, sql):
                if sql == "SELECT @@server_uuid":
                    return Row("instance-uuid")
                if sql.startswith("DROP DATABASE"):
                    test.exists = False
                return Row(1)

            def execute(self, sql, params):
                return Row(None if str(sql).startswith("SELECT SCHEMA_NAME") else int(test.exists))

        @contextmanager
        def session(*args):
            yield Conn()

        configured = replace(self.entry, url=self.entry.url.set(database="Warehouse"))
        with patch("readonly_mcp.mirror.admin_url", return_value=self.entry.url), patch("readonly_mcp.mirror.admin_session", session), patch("readonly_mcp.mirror.resolved_identity", resolved_identity):
            for action in ("drop", "status", "drop"):
                result = run_tool("db_mirror", {"action": action, "connection": "source"}, [configured]).structuredContent
                self.assertFalse(result["exists"])
                self.assertEqual(result["database"], canonical[1])


if __name__ == "__main__":
    unittest.main()
