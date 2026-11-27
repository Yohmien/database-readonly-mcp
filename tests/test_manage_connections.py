"""统一注册入口 manage_connections.py 的行为验证：原子写入、校验回滚、备份与恢复。"""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1]
CLI = SOURCE / "manage_connections.py"


class ManageConnectionsTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)
        self.config = self.root / "connections.json"
        self.db = self.root / "fixture.db"
        connection = sqlite3.connect(self.db)
        connection.execute("CREATE TABLE t (value TEXT)")
        connection.commit()
        connection.close()

    def cli(self, *arguments, expect=0):
        result = subprocess.run(
            [sys.executable, str(CLI), *arguments, "--config", str(self.config)],
            capture_output=True, text=True, encoding="utf-8",
            env={**os.environ, "DB_MCP_CONNECTIONS_FILE": str(self.config), "PYTHONIOENCODED": "utf-8"})
        output = result.stdout + result.stderr
        self.assertEqual(result.returncode, expect, output)
        return result

    def read_entries(self):
        return json.loads(self.config.read_text(encoding="utf-8"))["connections"]

    def test_add_list_remove_roundtrip(self):
        self.cli("add", "--type", "sqlite", "--name", "app-db", "--db-file", str(self.db),
                 "--alias", "app", "--max-rows", "250", "--timeout-seconds", "20")
        entries = self.read_entries()
        self.assertEqual([e["name"] for e in entries], ["app-db"])
        self.assertEqual(entries[0]["aliases"], ["app"])
        self.assertEqual(entries[0]["max_rows"], 250)
        self.assertEqual(entries[0]["timeout_seconds"], 20)
        listing = self.cli("list").stdout
        self.assertIn("app-db", listing)
        self.assertIn("sqlite", listing)
        self.assertIn("aliases=app", listing)
        self.cli("remove", "--name", "app-db")
        self.assertEqual(self.read_entries(), [])
        self.cli("remove", "--name", "app-db", expect=1)

    def test_duplicate_requires_replace(self):
        self.cli("add", "--type", "sqlite", "--name", "app-db", "--db-file", str(self.db))
        before = self.config.read_text(encoding="utf-8")
        result = self.cli("add", "--type", "sqlite", "--name", "app-db", "--db-file", str(self.db), expect=1)
        self.assertIn("已存在", result.stdout + result.stderr)
        self.assertEqual(self.config.read_text(encoding="utf-8"), before)
        self.cli("add", "--type", "sqlite", "--name", "app-db", "--db-file", str(self.db), "--replace")
        self.assertEqual(len(self.read_entries()), 1)

    def test_invalid_registration_rolls_back(self):
        self.cli("add", "--type", "sqlite", "--name", "app-db", "--db-file", str(self.db))
        before = self.config.read_text(encoding="utf-8")
        self.cli("add", "--type", "sqlite", "--name", "missing-file", "--db-file", str(self.root / "nope.db"), expect=1)
        self.cli("add", "--type", "mysql", "--name", "inline-creds",
                 "--url", "mysql://user:secret@127.0.0.1:3306", expect=1)
        self.cli("add", "--type", "postgresql", "--name", "bad-host", expect=1)
        self.cli("add", "--type", "sqlite", "--name", "extra-creds", "--db-file", str(self.db),
                 "--username-env", "X_USER", expect=1)
        self.assertEqual(self.config.read_text(encoding="utf-8"), before)

    def test_structured_registration_and_limits(self):
        self.cli("add", "--type", "mysql", "--name", "wms", "--host", "10.0.0.8", "--port", "3307",
                 "--database", "wms", "--username-env", "RO_USER", "--password-env", "RO_SECRET",
                 "--alias", "wms-ro")
        self.cli("add", "--type", "mysql", "--name", "cross", "--host", "10.0.0.8", "--allowed-databases", "wms, mes")
        self.cli("add", "--type", "mysql", "--name", "bad-cross", "--host", "10.0.0.8",
                 "--database", "wms", "--allowed-databases", "wms", expect=1)
        entries = self.read_entries()
        self.assertEqual(entries[0]["url"], "mysql://10.0.0.8:3307/wms")
        self.assertEqual(entries[0]["username_env"], "RO_USER")
        self.assertEqual(entries[1]["url"], "mysql://10.0.0.8:3306")
        self.assertEqual(entries[1]["allowed_databases"], ["wms", "mes"])

    def test_connect_verification(self):
        self.cli("add", "--type", "sqlite", "--name", "app-db", "--db-file", str(self.db))
        output = self.cli("test", "--name", "app-db", "--connect").stdout
        self.assertIn("配置校验：通过", output)
        self.assertIn("连接验证：通过", output)
        # 目标连接的凭据环境变量缺失时给出指引，而不是误报通过。
        self.cli("add", "--type", "mysql", "--name", "wms", "--host", "10.0.0.8", "--database", "wms",
                 "--username-env", "MANAGE_TEST_ABSENT_USER_7f3a", "--password-env", "MANAGE_TEST_ABSENT_password_7f3a")
        result = self.cli("test", "--name", "wms", "--connect", expect=1)
        self.assertIn("配置校验未通过", result.stdout + result.stderr)

    def test_restore_after_manual_corruption(self):
        self.cli("add", "--type", "sqlite", "--name", "one", "--db-file", str(self.db))
        self.cli("add", "--type", "sqlite", "--name", "two", "--db-file", str(self.db))
        self.config.write_text("{ broken", encoding="utf-8")
        result = self.cli("add", "--type", "sqlite", "--name", "three", "--db-file", str(self.db), expect=1)
        self.assertIn("恢复", result.stdout + result.stderr)
        self.cli("restore")
        self.assertEqual([e["name"] for e in self.read_entries()], ["one", "two"])
        # last-good 与写入前备份都已生成。
        self.assertTrue((self.root / "connections.last-good.json").is_file())
        self.assertTrue((self.root / "backups").is_dir())


if __name__ == "__main__":
    unittest.main()
