"""连接文件热重载：服务进程不重启，注册即时生效，损坏文件不中断服务。

覆盖多类型（sqlite 实连 + mysql 仅注册）、失败保留旧配置、configError 上报与 restore 恢复。
"""
import asyncio
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

SOURCE = Path(__file__).resolve().parents[1]
CLI = SOURCE / "manage_connections.py"
SERVER = SOURCE / "serve.py"
ENV_USER = "HOT_RELOAD_TEST_USER_9b1c"
ENV_PASS = "HOT_RELOAD_TEST_password_9b1c"


def run_cli(config, *arguments):
    result = subprocess.run(
        [sys.executable, str(CLI), *arguments, "--config", str(config)],
        capture_output=True, text=True, encoding="utf-8",
        env={**os.environ, "DB_MCP_CONNECTIONS_FILE": str(config), "PYTHONIOENCODED": "utf-8"})
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def make_fixture(path, table, value):
    connection = sqlite3.connect(path)
    connection.execute(f"CREATE TABLE {table} (value TEXT)")
    connection.execute(f"INSERT INTO {table} VALUES (:v)", {"v": value})
    connection.commit()
    connection.close()


class HotReloadTests(unittest.TestCase):
    def test_registration_hot_applies_and_failed_reload_keeps_config(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                first, second = root / "first.db", root / "second.db"
                make_fixture(first, "t", "alpha")
                make_fixture(second, "u", "beta")
                config = root / "connections.json"
                run_cli(config, "add", "--type", "sqlite", "--name", "sqlite-first", "--db-file", str(first))
                env = {**os.environ, "DB_MCP_CONNECTIONS_FILE": str(config), "PYTHONIOENCODED": "utf-8",
                       ENV_USER: "reader", ENV_PASS: "reader"}
                parameters = StdioServerParameters(command=sys.executable, args=[str(SERVER)], env=env)
                async with stdio_client(parameters) as (read, write):
                    async with ClientSession(read, write) as session:
                        await session.initialize()
                        result = await session.call_tool("db_connections", {})
                        self.assertEqual([c["name"] for c in result.structuredContent["connections"]], ["sqlite-first"])
                        self.assertIsNone(result.structuredContent["configError"])
                        result = await session.call_tool("db_query", {"connection": "sqlite-first", "sql": "SELECT value FROM t"})
                        self.assertEqual(result.structuredContent["rows"], [{"value": "alpha"}])

                        # 服务运行中通过统一入口热注册：第二个 sqlite 文件 + 一个 mysql 连接（仅注册）。
                        run_cli(config, "add", "--type", "sqlite", "--name", "sqlite-second", "--db-file", str(second))
                        run_cli(config, "add", "--type", "mysql", "--name", "mysql-listed", "--host", "127.0.0.1",
                                "--port", "3307", "--username-env", ENV_USER, "--password-env", ENV_PASS,
                                "--alias", "listed")
                        result = await session.call_tool("db_connections", {})
                        self.assertEqual([c["name"] for c in result.structuredContent["connections"]],
                                         ["sqlite-first", "sqlite-second", "mysql-listed"])
                        self.assertIsNone(result.structuredContent["configError"])
                        result = await session.call_tool("db_query", {"connection": "sqlite-second", "sql": "SELECT value FROM u"})
                        self.assertEqual(result.structuredContent["rows"], [{"value": "beta"}])
                        result = await session.call_tool("db_resolve", {"connection": "listed"})
                        self.assertEqual(result.structuredContent["name"], "mysql-listed")
                        self.assertEqual(result.structuredContent["dialect"], "mysql")

                        # 手工破坏文件（统一入口之外的操作）：旧配置继续服务并上报 configError。
                        config.write_text("{ broken", encoding="utf-8")
                        result = await session.call_tool("db_connections", {})
                        self.assertEqual(len(result.structuredContent["connections"]), 3)
                        self.assertTrue(result.structuredContent["configError"])
                        result = await session.call_tool("db_query", {"connection": "sqlite-first", "sql": "SELECT value FROM t"})
                        self.assertEqual(result.structuredContent["rows"], [{"value": "alpha"}])

                        # 从最近一次成功写入恢复，错误自动清除。
                        run_cli(config, "restore")
                        result = await session.call_tool("db_connections", {})
                        self.assertEqual([c["name"] for c in result.structuredContent["connections"]],
                                         ["sqlite-first", "sqlite-second", "mysql-listed"])
                        self.assertIsNone(result.structuredContent["configError"])
        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
