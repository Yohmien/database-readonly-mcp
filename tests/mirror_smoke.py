"""Explicit end-to-end validation: every database operation goes through stdio MCP."""
import asyncio
import json
import os
import sys
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

SOURCE_PROJECT = os.environ["DB_MCP_SMOKE_PROJECT"]
SOURCE_TABLE = os.environ["DB_MCP_SMOKE_TABLE"]
SERVICE = Path(__file__).resolve().parents[1]


async def main():
    parameters = StdioServerParameters(command=sys.executable, args=[str(SERVICE / "serve.py")], env=dict(os.environ))
    checks = []
    async with stdio_client(parameters) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = (await session.list_tools()).tools
            assert {t.name for t in tools} >= {"db_sql_check", "db_mirror", "db_query"}
            assert not next(t for t in tools if t.name == "db_mirror").annotations.readOnlyHint

            async def call(name, args, error=False):
                result = await session.call_tool(name, args)
                if bool(result.isError) != error:
                    raise AssertionError((name, result.structuredContent))
                return result.structuredContent

            route = {"project": SOURCE_PROJECT}
            async def mirror(action, **args):
                result = await call("db_mirror", {**route, "action": action, **args})
                checks.append({"action": action, "passed": True})
                print(json.dumps({"action": action, "passed": True}, ensure_ascii=False), flush=True)
                return result

            await call("db_query", {**route, "sql": "SHOW GRANTS"})
            resolved = await call("db_resolve", route)
            source = resolved["database"]
            table = SOURCE_TABLE.replace("`", "``")
            source_count = await call("db_query", {**route, "sql": f"SELECT COUNT(*) AS n FROM `{source.replace('`', '``')}`.`{table}`"})
            before = source_count["rows"][0]["n"]
            assert (await call("db_sql_check", {"sql": "SELECT (1", "dialect": "mysql"}))["status"] == "failed"
            assert (await call("db_sql_check", {"sql": "DROP TABLE t", "dialect": "mysql"}))["risks"]
            status = await mirror("status")
            assert status["configured"] and status["exists"] is False, "不接管正在使用的镜像"
            try:
                created = await mirror("create")
                assert created["database"] != source
                await mirror("create")
                migration = "CREATE TABLE IF NOT EXISTS mcp_probe(id INT PRIMARY KEY); INSERT INTO mcp_probe SELECT 1 WHERE NOT EXISTS(SELECT 1 FROM mcp_probe WHERE id=1);"
                await mirror("execute", sql=migration)
                await mirror("execute", sql=migration)
                result = await mirror("execute", sql="SELECT COUNT(*) AS n FROM mcp_probe;")
                assert result["output"].strip().splitlines()[-1] == "1"
                result = await mirror("execute", sql="SET @stmt='SELECT COUNT(*) AS n FROM mcp_probe'; PREPARE s FROM @stmt; EXECUTE s; DEALLOCATE PREPARE s;")
                assert result["output"].strip().splitlines()[-1] == "1"
                await mirror("execute", sql="CREATE VIEW mcp_view AS SELECT COUNT(*) AS n FROM mcp_probe;")
                result = await mirror("execute", sql="SELECT n FROM mcp_view;")
                assert result["output"].strip().splitlines()[-1] == "1"
                result = await mirror("execute", sql="DELIMITER $$\nCREATE PROCEDURE mcp_proc() SQL SECURITY INVOKER BEGIN SELECT COUNT(*) AS n FROM mcp_probe; END$$\nDELIMITER ;\nCALL mcp_proc(); DROP PROCEDURE mcp_proc;")
                assert result["output"].strip().splitlines()[-1] == "1"
                await mirror("execute", sql="CREATE PROCEDURE mcp_definer() SELECT COUNT(*) AS n FROM mcp_probe;")
                result = await mirror("execute", sql="CALL mcp_definer();")
                assert result["output"].strip().splitlines()[-1] == "1"
                await call("db_mirror", {**route, "action": "execute", "sql": f"UPDATE `{source.replace('`', '``')}`.`{table}` SET dict_id=dict_id WHERE 1=0;"}, error=True)
                checks.append({"action": "source_write_denied", "passed": True})
                await mirror("clear")
                result = await mirror("execute", sql="SELECT COUNT(*) AS n FROM mcp_probe;")
                assert result["output"].strip().splitlines()[-1] == "0"
                await mirror("init_empty", tables=[SOURCE_TABLE])
                result = await mirror("execute", sql=f"SELECT COUNT(*) AS n FROM `{table}`;")
                assert result["output"].strip().splitlines()[-1] == "0"
                await mirror("init_empty", tables=[SOURCE_TABLE])
                await mirror("clone", tables=[SOURCE_TABLE])
                result = await mirror("execute", sql=f"SELECT COUNT(*) AS n FROM `{table}`;")
                assert int(result["output"].strip().splitlines()[-1]) == before
                # No-op update on source was rejected; verify the source remains readable and unchanged.
                after = await call("db_query", {**route, "sql": f"SELECT COUNT(*) AS n FROM `{source.replace('`', '``')}`.`{table}`"})
                assert after["rows"][0]["n"] == before
            finally:
                await mirror("drop")
                assert (await mirror("status"))["exists"] is False
            print(json.dumps({"status": "passed", "checks": checks, "sourceRowsUnchanged": before}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
