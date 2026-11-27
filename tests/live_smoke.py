"""Explicit readonly smoke against the existing configured MySQL connection."""
import asyncio
import json
import os
import sys
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from readonly_mcp.config import project_targets

SOURCE_PROJECT = os.environ["DB_MCP_SMOKE_PROJECT"]
TABLE_PATTERN = os.environ.get("DB_MCP_SMOKE_TABLE_PATTERN", "")

async def main():
    env = dict(os.environ)
    source = Path(__file__).resolve().parents[1]
    parameters = StdioServerParameters(command=sys.executable, args=[str(source / "serve.py")], env=env)
    target = project_targets(SOURCE_PROJECT)[0]
    report = []
    async with stdio_client(parameters) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            listed = await session.list_tools()
            assert {tool.name for tool in listed.tools} >= {
                "db_query", "db_sql_check", "db_mirror", "db_sql_import", "db_script_test",
                "db_test_dataset", "db_task_result",
            }
            mutating = {"db_mirror", "db_sql_import", "db_script_test", "db_test_dataset"}
            assert all(tool.annotations.readOnlyHint for tool in listed.tools if tool.name not in mutating)
            assert all(not tool.annotations.readOnlyHint for tool in listed.tools if tool.name in mutating)
            report.append({"check": "tools/list", "tools": [tool.name for tool in listed.tools]})
            async def call(name, args, expected_error=False):
                result = await session.call_tool(name, args)
                assert bool(result.isError) == expected_error, (name, result)
                return result
            # This is also the backend's first SQL on each acquired connection.
            grants = await call("db_query", {"sql": "SHOW GRANTS"})
            assert "GRANT SELECT" in grants.content[0].text
            report.append({"check": "db_query SHOW GRANTS", "passed": True})
            result = await call("db_resolve", {"project": SOURCE_PROJECT})
            assert result.structuredContent["database"] == target.database
            report.append({"check": "project/profile resolution", "database": target.database})
            result = await call("db_query", {"sql": "SELECT :value AS n UNION ALL SELECT 2 UNION ALL SELECT 3", "params": {"value": 1}, "limit": 2})
            assert result.structuredContent["count"] == 2 and result.structuredContent["truncated"]
            report.append({"check": "bind parameters/row cap", "passed": True})
            result = await call("db_query", {"sql": "SELECT DATABASE() AS db", "database": target.database})
            assert result.structuredContent["rows"][0]["db"] == target.database
            report.append({"check": "hyphenated database selection/structured response", "passed": True})
            result = await call("db_tables", {"database": target.database, "pattern": TABLE_PATTERN, "limit": 2})
            assert result.structuredContent["tables"]
            table = result.structuredContent["tables"][0]["name"]
            result = await call("db_describe", {"database": target.database, "table": table})
            assert result.structuredContent["columns"]
            result = await call("db_query", {"sql": f"DESCRIBE `{target.database}`.`{table}`"})
            assert json.loads(result.content[0].text)
            report.append({"check": "tables/columns/keys/indexes/qualified DESCRIBE", "passed": True})
            result = await call("db_query", {"database": target.database, "sql": f"SELECT COUNT(*) AS n FROM `{target.database}`.`{table}` WHERE 1=0"})
            assert result.structuredContent["database"] == target.database
            report.append({"check": "explicit database selection with qualified SQL", "passed": True})
            result = await call("db_explain", {"sql": "SELECT 1"})
            assert result.structuredContent["rows"]
            report.append({"check": "non-executing explain", "passed": True})
            # Negative inputs must be rejected before reaching the database.
            for sql in ("DELETE FROM nonexistent_guard_probe", "SELECT 1 INTO OUTFILE 'nonexistent_guard_probe'", "SELECT 1; SELECT 2", "SELECT SLEEP(1)", "SELECT * FROM nonexistent_guard_probe FOR UPDATE"):
                result = await call("db_query", {"sql": sql}, True)
                assert result.structuredContent["kind"] == "rejected"
            report.append({"check": "write/file/lock/multi-statement/function rejection", "passed": True})
            result = await call('db_query', {'sql': 'SELECT 1 AS id, 2 AS id, 3 AS id__2'})
            assert result.structuredContent["rows"] == [{'id': 1, 'id__3': 2, 'id__2': 3}]
            report.append({'check': 'duplicate column collision preserved', 'passed': True})
            result = await call('db_query', {'sql': "SELECT UNIX_TIMESTAMP() AS ts, FROM_UNIXTIME(0) AS dt, FIELD('b','a','b') AS f, FIND_IN_SET('b','a,b') AS pos"})
            assert result.structuredContent['rows'][0]['f'] == 2
            assert result.structuredContent['rows'][0]['pos'] == 2
            result = await call('db_query', {'sql': 'SELECT JSON_ARRAYAGG(v) AS vals FROM (SELECT 1 AS v UNION ALL SELECT 2) x'})
            assert json.loads(result.structuredContent['rows'][0]['vals']) == [1,2]
            report.append({'check': 'five builtin functions execute', 'passed': True})
            first = await call('db_tables', {'database': target.database, 'limit': 5})
            offset = first.structuredContent['nextOffset']
            assert offset == 5
            second = await call('db_tables', {'database': target.database, 'limit': 5, 'offset': offset})
            assert not {t['name'] for t in first.structuredContent['tables']} & {t['name'] for t in second.structuredContent['tables']}
            report.append({'check': 'metadata nextOffset pagination', 'passed': True})
    print(json.dumps({"checks": report, "status": "passed"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
