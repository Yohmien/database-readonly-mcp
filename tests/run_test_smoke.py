"""Explicit stdio MCP validation for the bounded mirror test-process runner."""
import asyncio
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

SOURCE_PROJECT = os.environ["DB_MCP_SMOKE_PROJECT"]
SOURCE_TABLE = os.environ["DB_MCP_SMOKE_TABLE"]
SERVICE = Path(__file__).resolve().parents[1]


async def main():
    parameters = StdioServerParameters(
        command=sys.executable,
        args=[str(SERVICE / "serve.py")],
        env=dict(os.environ),
    )
    checks = []
    async with stdio_client(parameters) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = (await session.list_tools()).tools
            mirror_tool = next(tool for tool in tools if tool.name == "db_mirror")
            assert "run_test" in mirror_tool.inputSchema["properties"]["action"]["enum"]
            assert "command" in mirror_tool.inputSchema["properties"]
            task_tool = next(tool for tool in tools if tool.name == "db_task_result")
            assert "log_mode" in task_tool.inputSchema["properties"]
            assert "line_start" in task_tool.inputSchema["properties"]
            assert "include_result" in task_tool.inputSchema["properties"]
            checks.append({"check": "schema", "passed": True})
            print(json.dumps(checks[-1], ensure_ascii=False), flush=True)

            async def call(name, arguments, error=False):
                result = await session.call_tool(name, arguments)
                if bool(result.isError) != error:
                    raise AssertionError((name, result.structuredContent))
                return result.structuredContent

            route = {"project": SOURCE_PROJECT}

            async def mirror(action, **arguments):
                result = await call("db_mirror", {**route, "action": action, **arguments})
                checks.append({"check": action, "passed": True})
                print(json.dumps(checks[-1], ensure_ascii=False), flush=True)
                return result

            await call("db_query", {**route, "sql": "SHOW GRANTS"})
            source = (await call("db_resolve", route))["database"]
            source_table = SOURCE_TABLE.replace("`", "``")
            before = (
                await call(
                    "db_query",
                    {
                        **route,
                        "sql": f"SELECT COUNT(*) AS n FROM `{source.replace('`', '``')}`.`{source_table}`",
                    },
                )
            )["rows"][0]["n"]
            status = await mirror("status")
            assert status["configured"] and status["exists"] is False, "不接管正在使用的镜像"
            try:
                await mirror("create")
                probe_code = r"""
import os
import pymysql

connection = pymysql.connect(
    host=os.environ["DB_MCP_TEST_HOST"],
    port=int(os.environ["DB_MCP_TEST_PORT"]),
    user=os.environ["DB_MCP_TEST_USER"],
    password=os.environ["DB_MCP_TEST_PASSWORD"],
    database=os.environ["DB_MCP_TEST_DATABASE"],
    charset="utf8mb4",
    autocommit=True,
)
with connection.cursor() as cursor:
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS mcp_run_probe "
        "(id INT PRIMARY KEY, note VARCHAR(64) NOT NULL)"
    )
    cursor.execute(
        "INSERT INTO mcp_run_probe(id, note) VALUES (1, 'ok') "
        "ON DUPLICATE KEY UPDATE note=VALUES(note)"
    )
    cursor.execute("SELECT note FROM mcp_run_probe WHERE id=1")
    note = cursor.fetchone()[0]
connection.close()
print("MCP_RUN_TEST_OK=" + note)
print("PASSWORD=" + os.environ["DB_MCP_TEST_PASSWORD"])
print("PASSWORD_LEN=" + str(len(os.environ["DB_MCP_TEST_PASSWORD"])))
"""
                run = await mirror(
                    "run_test",
                    command=[sys.executable, "-c", probe_code],
                    cwd=str(SERVICE),
                )
                assert run["status"] == "passed" and run["exitCode"] == 0, run
                assert "MCP_RUN_TEST_OK=ok" in run["stdout"], run
                assert "PASSWORD=***" in run["stdout"], run
                assert not re.search(r"PASSWORD=(?!\*\*\*)", run["stdout"]), run
                checks.append({"check": "scoped_write_and_redaction", "passed": True})
                print(json.dumps(checks[-1], ensure_ascii=False), flush=True)

                result = await mirror("execute", sql="SELECT COUNT(*) AS n FROM mcp_run_probe;")
                assert result["output"].strip().splitlines()[-1] == "1"
                checks.append({"check": "execute_sees_child_write", "passed": True})
                print(json.dumps(checks[-1], ensure_ascii=False), flush=True)

                failed = await mirror(
                    "run_test",
                    command=[sys.executable, "-c", "raise SystemExit(7)"],
                    cwd=str(SERVICE),
                )
                assert failed["status"] == "failed" and failed["exitCode"] == 7, failed
                assert not failed["timedOut"]
                checks.append({"check": "nonzero_exit", "passed": True})
                print(json.dumps(checks[-1], ensure_ascii=False), flush=True)

                timeout_code = (
                    "import subprocess, sys, time\n"
                    "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])\n"
                    "print(child.pid, flush=True)\n"
                    "time.sleep(30)\n"
                )
                timed = await mirror(
                    "run_test",
                    command=[sys.executable, "-c", timeout_code],
                    cwd=str(SERVICE),
                    timeout_seconds=2,
                )
                assert timed["status"] == "timed_out" and timed["timedOut"], timed
                child_pid = int(timed["stdout"].splitlines()[0])
                listing = subprocess.run(
                    ["tasklist", "/FO", "CSV", "/NH"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                ).stdout
                assert f'"{child_pid}"' not in listing
                checks.append({"check": "timeout_tree_cleanup", "passed": True})
                print(json.dumps(checks[-1], ensure_ascii=False), flush=True)

                long_code = (
                    "import time\n"
                    "print('LONG_START', flush=True)\n"
                    "time.sleep(12)\n"
                    "print('LONG_DONE', flush=True)\n"
                )
                started = time.monotonic()
                running = await mirror(
                    "run_test",
                    command=[sys.executable, "-c", long_code],
                    cwd=str(SERVICE),
                )
                elapsed = time.monotonic() - started
                assert running["status"] == "running", running
                assert running.get("taskId") and running.get("logPath"), running
                assert elapsed < 12, elapsed
                assert Path(running["logPath"]).is_file(), running
                checks.append({
                    "check": "run_test_fast_return",
                    "passed": True,
                    "elapsedSeconds": round(elapsed, 2),
                })
                print(json.dumps(checks[-1], ensure_ascii=False), flush=True)

                final = None
                for _ in range(60):
                    final = await call("db_task_result", {"task_id": running["taskId"]})
                    if final["status"] != "running":
                        break
                    await asyncio.sleep(1)
                assert final["status"] == "passed", final
                assert final["resultSummary"]["exitCode"] == 0, final
                assert final["text"] == "", final
                checks.append({"check": "task_result_summary", "passed": True})
                print(json.dumps(checks[-1], ensure_ascii=False), flush=True)

                full = await call(
                    "db_task_result",
                    {"task_id": running["taskId"], "log_mode": "full"},
                )
                assert "LONG_START" in full["text"] and "LONG_DONE" in full["text"], full
                lines = full["text"].splitlines()
                line_number = next(
                    index for index, line in enumerate(lines, 1) if "LONG_DONE" in line
                )
                selected = await call(
                    "db_task_result",
                    {
                        "task_id": running["taskId"],
                        "line_start": line_number,
                        "line_end": line_number,
                    },
                )
                assert selected["lineCount"] == 1 and "LONG_DONE" in selected["text"], selected
                complete = await call(
                    "db_task_result",
                    {"task_id": running["taskId"], "include_result": True},
                )
                assert "LONG_DONE" in complete["result"]["stdout"], complete
                checks.append({"check": "task_result_log_reads", "passed": True})
                print(json.dumps(checks[-1], ensure_ascii=False), flush=True)
            finally:
                await mirror("drop")
                assert (await mirror("status"))["exists"] is False

            missing = await call(
                "db_mirror",
                {
                    **route,
                    "action": "run_test",
                    "command": [sys.executable, "-c", "print(1)"],
                    "cwd": str(SERVICE),
                },
                error=True,
            )
            assert "镜像库不存在" in missing["error"], missing
            checks.append({"check": "missing_mirror_rejected", "passed": True})
            print(json.dumps(checks[-1], ensure_ascii=False), flush=True)

            after = (
                await call(
                    "db_query",
                    {
                        **route,
                        "sql": f"SELECT COUNT(*) AS n FROM `{source.replace('`', '``')}`.`{source_table}`",
                    },
                )
            )["rows"][0]["n"]
            assert after == before
            checks.append({"check": "source_unchanged", "passed": True, "rows": before})
            print(json.dumps({"status": "passed", "checks": checks}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
