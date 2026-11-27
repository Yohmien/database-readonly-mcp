from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import threading
import time
from pathlib import Path

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import CallToolResult, TextContent, Tool, ToolAnnotations
from pymysql.err import MySQLError

from .backend import Backend
from .config import Rejected, load_connections, resolve
from .sql_check import check_sql
from .mirror import (export_test_dataset, extend_test_dataset, load_test_dataset, prepare_test_dataset,
                     run_mirror, run_script_test)
from .sql_import import DEFAULT_CHUNK_MB, DEFAULT_THREADS, MAX_CHUNK_MB, run_sql_import
from .runlog import HEARTBEAT_SECONDS, TaskLog, read_task


FAST_RETURN_SECONDS = 10


ROUTING = {"connection": {"type": "string", "description": "Connection name/alias"},
           "datasource_url": {"type": "string", "description": "Target URL/JDBC URL; credentials ignored"},
           "project": {"type": "string", "description": "Spring project path"},
           "profile": {"type": "string"}, "database": {"type": "string"}, "dialect": {"type": "string", "enum": ["mysql", "mariadb", "postgresql", "mssql", "oracle", "sqlite"]}}
QUERY = {"sql": {"type": "string"}, "params": {"type": "object", "description": ":name scalar binds"}, "limit": {"type": "integer", "minimum": 1, "maximum": 10000, "default": 100}}
META = {"schema": {"type": "string"}, "table": {"type": "string"}, "pattern": {"type": "string", "description": "Name substring, case-insensitive"}, "limit": {"type": "integer", "minimum": 1, "maximum": 1000}}
TOOLS = [
    ("db_sql_check", "SQLGlot 离线静态解析与风险提示；不执行 SQL，不证明幂等性。", {"sql": {"type": "string"}, "dialect": ROUTING["dialect"]}, ["sql", "dialect"]),
    ("db_mirror", "唯一镜像入口：同引擎新增/清空/空数据初始化/复制/执行SQL/运行受控测试进程/删除。来源库只读。大库优先用 tables 指定测试所需基础表，单表数据量仍大时用 init_empty 后导入必要样本，避免全库克隆超时。超过 10 秒返回 taskId 与日志路径，用 db_task_result 继续读取。", {**ROUTING, "action": {"type": "string", "enum": ["status", "create", "clear", "init_empty", "clone", "execute", "run_test", "drop"]}, "sql": {"type": "string"}, "tables": {"type": "array", "items": {"type": "string"}}, "command": {"type": "array", "items": {"type": "string", "minLength": 1, "maxLength": 4096}, "minItems": 1, "maxItems": 64}, "cwd": {"type": "string"}, "timeout_seconds": {"type": "integer", "minimum": 1, "description": "run_test 可省略，省略时不限制测试进程时长；其他镜像动作默认 60 秒"}, "mirror_name": {"type": "string", "description": "Test-only mirror override; requires DB_MCP_MIRROR_ALLOW_NAMED=1"}}, ["action"]),
    ("db_task_result", "读取超过 10 秒的数据库 MCP 后台任务：默认只返回状态、最近进度和结果摘要；log_mode=tail 省略 offset 取末尾，传 offset 续读；log_mode=full 读取全部；line_start/line_end 读取指定行；确需完整结果时显式 include_result=true。", {"task_id": {"type": "string"}, "offset": {"type": "integer", "minimum": 0}, "max_bytes": {"type": "integer", "minimum": 1, "maximum": 65536}, "include_result": {"type": "boolean", "default": False}, "log_mode": {"type": "string", "enum": ["summary", "tail", "full"], "default": "summary"}, "line_start": {"type": "integer", "minimum": 1}, "line_end": {"type": "integer", "minimum": 1}}, ["task_id"]),
    ("db_script_test", "完整镜像脚本测试：自动准备镜像，用真实 MySQL 客户端执行首次和重复脚本，按需执行 verify_sql 核对结果，最后强制删除镜像。适用于 DELIMITER、存储过程、函数、触发器等 SQLGlot 不支持包装。", {**ROUTING, "sql": {"type": "string"}, "verify_sql": {"type": "string"}, "prepare": {"type": "string", "enum": ["empty", "init_empty", "clone"]}, "tables": {"type": "array", "items": {"type": "string"}}, "timeout_seconds": {"type": "integer", "minimum": 1}}, ["sql", "prepare"]),
    ("db_test_dataset", "按业务条件剪枝并导出测试数据集，或直接载入唯一镜像后补充基础数据和执行验证 SQL。先由 db_query 等只读事实判定是否为剪枝缺口；确认后才用 extend 增量补充。extend 将本轮已确认的缺口合并追加到现有镜像和同一数据包，随即用 verify_sql 或测试入口隔离复验；失败轮次同样写入清单并标记状态，随后回源库定位新缺口再 extend，循环至镜像事实与源库一致或排除剪枝原因。prepare/export 的 schema_mode=all 一次保留全库结构并只复制指定数据，不重建全量数据集。", {**ROUTING, "action": {"type": "string", "enum": ["prepare", "export", "load", "extend"]}, "schema_mode": {"type": "string", "enum": ["selected", "all"], "description": "all 保留全库结构，未指定数据条件的表使用 1=0"}, "tables": {"type": "array", "items": {"type": "string"}}, "prune": {"type": "object", "description": "业务表到单条 SQL WHERE 条件的映射；至少一项"}, "supplement_tables": {"type": "array", "items": {"type": "string"}, "description": "完整复制的小型依赖表"}, "supplement_sql": {"type": "string", "description": "数据载入后执行的可重复 SQL/客户端脚本"}, "verify_sql": {"type": "string", "description": "数据载入或本轮增量后执行的隔离验证查询；失败后回到源库只读交叉验证"}, "output_dir": {"type": "string", "description": "export 使用的绝对新目录"}, "input_dir": {"type": "string", "description": "load/extend 使用的导出目录"}, "threads": {"type": "integer", "minimum": 1, "maximum": 16}, "bytes_per_chunk": {"type": "string"}, "timeout_seconds": {"type": "integer", "minimum": 1}}, ["action"]),
    ("db_sql_import", "将用户提供的 MySQL SQL 文件导入新库。用于初始化数据库或测试库时直接走本工具，不论文件大小、仅 DDL 或普通顺序 SQL；不要先用 rg、文本读取拆解或直接执行该文件。数据库名缺省取 SQL 文件名（去掉 .sql），可用 database_name 指定；status/drop 用同一定位。按表结构和 INSERT 语句分块并行写入，适合多 GB 文件；失败自动删除新库。导入库成功后保留供后续查询；镜像库测试结束即删，导入库本身仅在用户要求时清理，超过 1 GB 的导入库删除前必须先征得用户同意并传 confirm_large_drop=true。drop 只删除本工具登记的数据库。", {**ROUTING, "action": {"type": "string", "enum": ["import", "status", "drop"], "default": "import"}, "sql_path": {"type": "string", "description": "绝对 SQL 文件路径；import 必需，缺省库名取去掉 .sql 的文件名"}, "database_name": {"type": "string", "description": "导入库名；缺省取 SQL 文件名。status/drop 可单独用它定位导入库"}, "threads": {"type": "integer", "minimum": 1, "maximum": 16, "default": DEFAULT_THREADS}, "chunk_mb": {"type": "integer", "minimum": 1, "maximum": MAX_CHUNK_MB, "default": DEFAULT_CHUNK_MB}, "verify_sql": {"type": "string", "description": "导入后可选验证 SQL，仅使用新库权限"}, "confirm_large_drop": {"type": "boolean", "default": False, "description": "删除超过 1 GB 的导入库前必须先征得用户同意；确认后才传 true"}, "timeout_seconds": {"type": "integer", "minimum": 1, "default": 3600}}, ["action"]),
    ("db_query", "Read SQL; routes directly, no resolve preflight. arrays avoids repeated column names.", {**ROUTING, **QUERY, "row_format": {"type": "string", "enum": ["objects", "arrays", "both"], "default": "objects"}}, ["sql"]),
    ("db_connections", "Discover registered targets only when the target is unknown; no secrets.", {}, []),
    ("db_resolve", "Diagnose routing; queries resolve targets themselves.", ROUTING, []),
    ("db_schemas", "Schema names; continue with nextOffset as offset.", {**ROUTING, "limit": META["limit"], "offset": {"type":"integer", "minimum":0}}, []),
    ("db_tables", "Table/view names; continue with nextOffset as offset.", {**ROUTING, "schema": META["schema"], "pattern": META["pattern"], "limit": META["limit"], "offset": {"type":"integer", "minimum":0}}, []),
    ("db_describe", "Known table's columns; full adds keys/indexes. No table-list preflight needed.", {**ROUTING, "schema": META["schema"], "table": META["table"], "detail": {"type": "string", "enum": ["columns", "full"], "default": "columns"}}, ["table"]),
    ("db_explain", "Non-executing plan: MySQL/MariaDB/PostgreSQL/SQLite. No ANALYZE.", {**ROUTING, **QUERY}, ["sql"]),
]
for name, _, properties, _ in TOOLS:
    if name == "db_mirror":
        properties["clone_engine"] = {"type": "string", "enum": ["mysqldump", "mysqlsh"],
                                      "description": "仅用于 clone。mysqlsh 并行克隆需显式 tables，且来源数据在导出期间应静止。"}


def payload(value):
    return CallToolResult(content=[TextContent(type="text", text=json.dumps(value, ensure_ascii=False, separators=(",", ":")))], structuredContent=value if isinstance(value, dict) else {"result": value}, isError=False)


def format_rows(result, row_format):
    formatted = dict(result)
    if row_format not in {"objects", "arrays", "both"}:
        raise Rejected("Unknown row_format")
    if row_format == "arrays":
        formatted.pop("rows")
    elif row_format == "objects" and len(set(result["columns"])) == len(result["columns"]):
        formatted.pop("values")
    return formatted


def file_signature(path: Path):
    try:
        status = path.stat()
    except OSError:
        return None
    return (status.st_mtime_ns, status.st_size)


def make_reload(path: Path, entries):
    """Wrap the startup connections so later file changes apply without a restart.

    每次工具调用前检查连接文件签名（mtime+size），变更时用服务同一套 load_connections
    重新加载。重载与数据库类型无关：六种方言都走同一注册与校验路径。文件缺失或重载
    失败时保留已加载配置并记录错误，下一次调用自动重试；错误通过 db_connections 的
    configError 字段暴露。
    """
    state = {"signature": file_signature(path), "entries": entries, "error": None}
    lock = threading.Lock()

    def current():
        signature = file_signature(path)
        if signature is None or signature == state["signature"]:
            return state["entries"], state["error"]
        with lock:
            signature = file_signature(path)
            if signature is None or signature == state["signature"]:
                return state["entries"], state["error"]
            try:
                reloaded = load_connections(path)
            except Exception as error:
                state["error"] = str(error)
                print(f"Readonly MCP connections reload failed; keeping previous configuration: {error}", file=sys.stderr)
                return state["entries"], state["error"]
            state.update(signature=signature, entries=reloaded, error=None)
            return reloaded, None

    return current


def task_summary(arguments):
    keys = ("connection", "project", "profile", "database", "dialect", "action",
            "sql_path", "input_dir", "output_dir", "mirror_name", "prepare", "schema_mode")
    summary = {key: arguments[key] for key in keys if arguments.get(key) is not None}
    command = arguments.get("command")
    if isinstance(command, list) and command:
        summary["command"] = Path(command[0]).name
        summary["commandArgs"] = len(command)
    if isinstance(arguments.get("sql"), str):
        summary["sqlChars"] = len(arguments["sql"])
    if isinstance(arguments.get("verify_sql"), str):
        summary["verifySqlChars"] = len(arguments["verify_sql"])
    if isinstance(arguments.get("tables"), list):
        summary["tables"] = len(arguments["tables"])
    if isinstance(arguments.get("prune"), dict):
        summary["pruneTables"] = len(arguments["prune"])
    return summary


def dispatch(name, arguments, entries, config_error, task):
    if name == "db_sql_check":
        return check_sql(arguments["sql"], arguments["dialect"])
    if name == "db_connections":
        return {"connections": [e.public() for e in entries], "configError": config_error}
    routing = {k: v for k, v in arguments.items() if k in ROUTING}
    config = resolve(entries, **routing)
    if name == "db_script_test":
        if config_error:
            raise Rejected("连接配置重载失败，脚本测试已停止")
        return run_script_test(config, arguments, job=task)
    if name == "db_test_dataset":
        if config_error:
            raise Rejected("连接配置重载失败，测试数据集操作已停止")
        if arguments["action"] == "prepare":
            return prepare_test_dataset(config, arguments, job=task)
        if arguments["action"] == "export":
            return export_test_dataset(config, arguments, job=task)
        if arguments["action"] == "extend":
            return extend_test_dataset(config, arguments, job=task)
        return load_test_dataset(config, arguments, job=task)
    if name == "db_sql_import":
        if config_error:
            raise Rejected("连接配置重载失败，SQL 导入已停止")
        return run_sql_import(config, arguments, job=task)
    if name == "db_mirror":
        if config_error:
            raise Rejected("连接配置重载失败，镜像操作已停止")
        return run_mirror(config, arguments, job=task)
    if name == "db_resolve":
        return config.public()
    backend = Backend(config)
    if name in {"db_query", "db_explain"}:
        sql = arguments["sql"]
        if name == "db_explain":
            if not re.match(r"\s*(SELECT|WITH)\b", sql, re.I):
                raise Rejected("db_explain requires SELECT/WITH")
            sql = ("EXPLAIN QUERY PLAN " if backend.dialect == "sqlite" else "EXPLAIN ") + sql
        limit = arguments.get("limit")
        if limit is None:
            limit = min(100, config.max_rows)
        result = backend.query(sql, arguments.get("params"), limit)
        return format_rows(result, arguments.get("row_format", "objects"))
    operation = {"db_schemas": "schemas", "db_tables": "tables", "db_describe": "describe"}[name]
    return backend.metadata(operation, arguments.get("schema"), arguments.get("table"),
                            arguments.get("pattern"), arguments.get("limit"),
                            detail=arguments.get("detail", "columns"),
                            offset=arguments.get("offset", 0))


def _heartbeat(task, done):
    while not done.wait(HEARTBEAT_SECONDS):
        task.progress(f"elapsed={int(time.monotonic() - task.started_monotonic)}s")


def run_tool(name, arguments, entries, config_error=None):
    specs = {n: (props, required) for n, _, props, required in TOOLS}
    if name not in specs:
        raise Rejected("Unknown tool")
    props, required = specs[name]
    if not isinstance(arguments, dict) or set(arguments) - props.keys() or any(k not in arguments for k in required):
        raise Rejected("Unexpected or missing tool argument")
    for key, value in arguments.items():
        kind = props[key]["type"]
        if (kind == "string" and not isinstance(value, str) or kind == "integer" and (not isinstance(value, int) or isinstance(value, bool)) or kind == "boolean" and not isinstance(value, bool) or kind == "object" and not isinstance(value, dict) or kind == "array" and (not isinstance(value, list) or any(not isinstance(v, str) for v in value))):
            raise Rejected("Invalid argument type: " + key)
        if "enum" in props[key] and value not in props[key]["enum"]:
            raise Rejected("Invalid argument value: " + key)
        if kind == "integer" and (value < props[key].get("minimum", value) or value > props[key].get("maximum", value)):
            raise Rejected("Invalid argument range: " + key)
    if name == "db_task_result":
        return payload(read_task(
            arguments["task_id"],
            arguments.get("offset"),
            arguments.get("max_bytes", 16 * 1024),
            arguments.get("include_result", False),
            arguments.get("log_mode", "summary"),
            arguments.get("line_start"),
            arguments.get("line_end"),
        ))

    task = TaskLog(name, arguments.get("action"), task_summary(arguments))
    outcome = {}
    done = threading.Event()

    def work():
        try:
            outcome["result"] = dispatch(name, arguments, entries, config_error, task)
        except BaseException as error:
            outcome["error"] = error
            outcome["info"] = safe_error(error)
            try:
                task.log("event=error " + json.dumps(outcome["info"], ensure_ascii=False, default=str))
            except Exception:
                pass
        finally:
            try:
                task.finish(result=outcome.get("result"), error=outcome.get("info"))
            except Exception:
                pass
            done.set()

    threading.Thread(target=work, name="dbmcp-" + task.task_id, daemon=True).start()
    threading.Thread(target=_heartbeat, args=(task, done), daemon=True).start()
    if done.wait(FAST_RETURN_SECONDS):
        if "error" in outcome:
            raise outcome["error"]
        return payload(task.with_reference(outcome["result"]))
    task.spill()
    if done.wait(0):
        if "error" in outcome:
            raise outcome["error"]
        return payload(task.with_reference(outcome["result"]))
    return payload(task.running_payload())


def safe_error(error):
    if isinstance(error, Rejected):
        return {"kind": "rejected", "error": str(error)}
    # Driver errors may echo credentials, SQL text, bind values or DSNs; expose only code/class.
    original = getattr(error, "orig", error)
    code = getattr(original, "sqlstate", None) or getattr(original, "pgcode", None)
    if code is None and getattr(original, "args", ()) and isinstance(original.args[0], (int, str)):
        first = str(original.args[0])
        code = first if re.fullmatch(r"[A-Z0-9]{4,8}", first) else None
    if code is not None:
        code = str(code)
        code = code if re.fullmatch(r"[A-Z0-9]{4,8}", code) else None
    info = {"kind": "database_error", "error": "数据库操作失败", "type": type(original).__name__, "code": code}
    if isinstance(original, MySQLError):
        args = original.args
        errno = args[0] if args and type(args[0]) is int else None
        if errno is not None:
            info["errno"] = errno
        messages = {
            1044: "只读账号无权访问目标数据库，请核对目标库及该账号的 SELECT 授权",
            1045: "数据库身份验证失败，请核对 MCP 只读账号凭据及允许连接的主机",
            1049: "目标数据库不存在，请核对当前 Profile 与数据库名称",
            1064: "数据库 SQL 语法错误，请核对语句及服务器版本",
            1142: "只读账号无权执行该命令，请核对所需读取权限",
            1143: "只读账号无权读取目标字段，请核对字段读取权限",
            1193: "数据库不支持当前会话变量，请核对服务器类型与版本",
            1227: "数据库拒绝执行当前操作，请核对所需权限",
        }
        info["error"] = messages.get(errno, info["error"])
    return info


async def main():
    path = Path(os.environ.get("DB_MCP_CONNECTIONS_FILE", str(Path(__file__).resolve().parents[1] / "connections.json")))
    try:
        entries = load_connections(path)
    except Exception:
        print("Readonly MCP configuration invalid; check connection file and environment references", file=sys.stderr)
        raise SystemExit(1) from None
    current = make_reload(path, entries)
    server = Server("database-readonly-mcp", version="1.7.0")
    @server.list_tools()
    async def list_tools():
        mutating = {"db_mirror", "db_script_test", "db_test_dataset", "db_sql_import"}
        return [Tool(name=n, description=d, inputSchema={"type": "object", "properties": p, "required": r, "additionalProperties": False}, annotations=ToolAnnotations(readOnlyHint=n not in mutating, destructiveHint=n in mutating, idempotentHint=n not in mutating, openWorldHint=False)) for n, d, p, r in TOOLS]
    @server.call_tool()
    async def call_tool(name, arguments):
        try:
            def dispatch():
                connections, config_error = current()
                return run_tool(name, arguments or {}, connections, config_error)
            return await asyncio.to_thread(dispatch)
        except Exception as error:
            info = safe_error(error)
            return CallToolResult(content=[TextContent(type="text", text=json.dumps(info))], structuredContent=info, isError=True)
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
