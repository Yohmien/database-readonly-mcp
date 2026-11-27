"""数据库只读 MCP 的统一连接注册入口。

代替手工编辑 connections.json：注册、移除、恢复、校验全部走本脚本，
写入前用服务同一套 load_connections 校验，失败自动回滚，不落盘。

- 原子写入（临时文件 + os.replace），运行中的服务不会读到半截配置；
- 每次写入前把原文件备份到连接文件同级的 backups/<UTC时间戳>/；
- 每次写入成功后保存最近一次正确状态到 <连接文件>.last-good.json，供 restore 恢复；
- 六种数据库类型统一入口，URL 由参数生成或校验，凭据只引用环境变量名，不写值；
- 配合服务的热重载（1.3 起），注册后运行中的服务在下一次工具调用自动生效。

用法（使用服务自带的隔离环境）：
  .venv\\Scripts\\python.exe manage_connections.py list
  .venv\\Scripts\\python.exe manage_connections.py add --type sqlite --name app-db --db-file D:/data/app.db
  .venv\\Scripts\\python.exe manage_connections.py add --type mysql --name wms --host 127.0.0.1 --port 3306 --database wms --username-env RO_USER --password-env RO_PASS --alias wms-ro
  .venv\\Scripts\\python.exe manage_connections.py add --type mysql --name cross --host 127.0.0.1 --allowed-databases wms,mes
  .venv\\Scripts\\python.exe manage_connections.py remove --name app-db
  .venv\\Scripts\\python.exe manage_connections.py test --name wms --connect
  .venv\\Scripts\\python.exe manage_connections.py restore
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from readonly_mcp.config import Rejected, load_connections, target_from_url

DIALECTS = ("mysql", "mariadb", "postgresql", "mssql", "oracle", "sqlite")
DEFAULT_PORTS = {"mysql": 3306, "mariadb": 3306, "postgresql": 5432, "mssql": 1433, "oracle": 1521}
CREDENTIAL_KEYS = ("username_env", "password_env", "url_env")


def connections_file(args):
    override = getattr(args, "config", None)
    return Path(override or os.environ.get("DB_MCP_CONNECTIONS_FILE")
                or Path(__file__).resolve().parent / "connections.json")


def read_entries(file: Path):
    if not file.exists():
        return []
    try:
        data = json.loads(file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise SystemExit(f"连接文件不是有效 JSON（第 {error.lineno} 行：{error.msg}）；可运行 restore 从最近一次成功写入恢复") from None
    if not isinstance(data, dict) or set(data) != {"connections"} or not isinstance(data.get("connections"), list):
        raise SystemExit("连接文件结构异常（应只包含 connections 数组）；可运行 restore 从最近一次成功写入恢复") from None
    return list(data["connections"])


def referenced_env_vars(entries):
    return sorted({entry[key] for entry in entries for key in CREDENTIAL_KEYS if entry.get(key)})


def server_env_blocks():
    """各 harness 实际传给 database_readonly 服务进程的环境变量名集合。"""
    blocks = {}
    magpie = Path.home() / ".config" / "magpie" / "library.json"
    if magpie.is_file():
        try:
            for item in json.loads(magpie.read_text(encoding="utf-8")).get("mcp", []):
                if item.get("name") == "database_readonly":
                    blocks["magpie library.json"] = set(item.get("env", {}))
                    break
        except Exception:
            pass
    codex = Path.home() / ".codex" / "config.toml"
    if codex.is_file():
        try:
            import tomllib
            block = tomllib.loads(codex.read_text(encoding="utf-8")).get("mcp_servers", {}).get("database_readonly", {})
            blocks["codex config.toml"] = set(block.get("env", {}))
        except Exception:
            pass
    return blocks


def report_server_env(entries):
    """提醒凭据引用的环境变量是否对服务进程可见；缺失时服务会暂缓加载新配置。"""
    refs = referenced_env_vars(entries)
    if not refs:
        return
    blocks = server_env_blocks()
    visible = set().union(*blocks.values(), os.environ.keys()) if blocks else set(os.environ.keys())
    missing = [name for name in refs if name not in visible]
    if not missing:
        print(f"凭据引用的环境变量（{', '.join(refs)}）对服务可见。")
        return
    print(f"警告：环境变量 {', '.join(missing)} 未出现在 harness 传给服务的 env 中；"
          "热重载会保留旧配置并报 configError，直到这些变量对服务进程可见（变量值由管理员在 harness 配置中提供）。")
    if "magpie library.json" in blocks:
        print("  magpie：编辑 ~/.config/magpie/library.json 中 database_readonly 的 env 块。")
    if "codex config.toml" in blocks:
        print("  codex：编辑 ~/.codex/config.toml 中 [mcp_servers.database_readonly.env]。")


def write_entries(file: Path, entries):
    """校验通过才原子落盘：任何失败都不改动现有文件。"""
    payload = json.dumps({"connections": entries}, ensure_ascii=False, indent=2) + "\n"
    with tempfile.TemporaryDirectory() as folder:
        candidate = Path(folder) / "connections.json"
        candidate.write_text(payload, encoding="utf-8")
        try:
            with patch.dict(os.environ, {name: "validation" for name in referenced_env_vars(entries)}):
                load_connections(candidate)
        except Exception as error:
            raise SystemExit(f"校验未通过，已取消写入（现有文件未改动）：{error}") from None
    file.parent.mkdir(parents=True, exist_ok=True)
    if file.exists():
        backup = file.parent / "backups" / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        backup.mkdir(parents=True, exist_ok=True)
        shutil.copy2(file, backup / "connections.json")
    handle, tmp_name = tempfile.mkstemp(dir=file.parent, prefix=file.name + ".", suffix=".tmp")
    os.close(handle)
    Path(tmp_name).write_text(payload, encoding="utf-8")
    os.replace(tmp_name, file)
    shutil.copy2(file, file.with_suffix(".last-good.json"))
    print(f"已写入 {file}（共 {len(entries)} 个连接）；运行中的服务在下一次工具调用自动加载，无需重连。")


def build_url(args):
    if args.url:
        target = target_from_url(args.url)
        if target.dialect != args.type:
            raise SystemExit(f"--url 解析为 {target.dialect}，与 --type {args.type} 不一致")
        return args.url
    if args.type == "sqlite":
        if not args.db_file:
            raise SystemExit("sqlite 需要 --db-file 指向已存在的数据库文件")
        db_path = Path(args.db_file).expanduser().resolve()
        if not db_path.is_file():
            raise SystemExit(f"sqlite 数据库文件不存在：{db_path}（服务端同样只打开已有文件）")
        return "sqlite:///" + db_path.as_posix()
    if not args.host:
        raise SystemExit("需要 --host（或用 --url 提供完整地址）")
    port = args.port or DEFAULT_PORTS[args.type]
    url = f"{args.type}://{args.host}:{port}"
    if args.type == "oracle" and args.service_name:
        url += "/" + args.service_name
    elif args.database:
        url += "/" + args.database
    return url


def build_entry(args):
    if args.type == "sqlite" and (args.username_env or args.password_env):
        raise SystemExit("sqlite 无需账号密码，不要传 --username-env/--password-env")
    entry = {"name": args.name, "url": build_url(args)}
    if args.username_env:
        entry["username_env"] = args.username_env
    if args.password_env:
        entry["password_env"] = args.password_env
    if args.alias:
        entry["aliases"] = args.alias
    if args.max_rows is not None:
        entry["max_rows"] = args.max_rows
    if args.timeout_seconds is not None:
        entry["timeout_seconds"] = args.timeout_seconds
    if args.allowed_databases:
        if args.type not in ("mysql", "mariadb"):
            raise SystemExit("--allowed-databases 仅用于 mysql/mariadb 跨库连接")
        if args.database:
            raise SystemExit("已固定 --database 的连接不支持 --allowed-databases（跨库连接不传 --database）")
        entry["allowed_databases"] = [item.strip() for item in args.allowed_databases.split(",") if item.strip()]
    return entry


def command_add(args):
    file = connections_file(args)
    entries = read_entries(file)
    if any(e.get("name") == args.name for e in entries) and not args.replace:
        raise SystemExit(f"连接 {args.name} 已存在；确认覆盖请加 --replace，或先 remove")
    entry = build_entry(args)
    target = target_from_url(entry["url"])
    location = target.database if target.dialect == "sqlite" else \
        f"{target.host or '-'}:{target.port or '-'}" + (f"/{target.database}" if target.database else "")
    print(f"注册 {entry['name']}：类型={target.dialect} 目标={location}")
    merged = [e for e in entries if e.get("name") != args.name] + [entry]
    write_entries(file, merged)
    report_server_env(merged)


def command_remove(args):
    file = connections_file(args)
    entries = read_entries(file)
    kept = [e for e in entries if e.get("name") != args.name]
    if len(kept) == len(entries):
        raise SystemExit(f"连接 {args.name} 不存在")
    write_entries(file, kept)
    print(f"已移除 {args.name}。")


def command_restore(args):
    file = connections_file(args)
    last_good = file.with_suffix(".last-good.json")
    sources = []
    if last_good.is_file():
        sources.append(("最近一次成功写入", last_good))
    backups = file.parent / "backups"
    if backups.is_dir():
        for folder in sorted(backups.iterdir(), reverse=True):
            snapshot = folder / "connections.json"
            if snapshot.is_file():
                sources.append((f"写入前备份 {folder.name}", snapshot))
                break
    if not sources:
        raise SystemExit("没有可用的恢复来源（.last-good 或 backups/）")
    label, source = sources[0]
    try:
        entries = json.loads(source.read_text(encoding="utf-8"))["connections"]
    except Exception as error:
        raise SystemExit(f"恢复来源不可用（{label}）：{error}") from None
    write_entries(file, entries)
    print(f"已从{label}恢复 {len(entries)} 个连接。")


def command_list(args):
    file = connections_file(args)
    print(f"连接文件：{file}")
    if not file.exists():
        print("（文件不存在，尚无连接注册）")
        return
    entries = read_entries(file)
    if not entries:
        print("（尚无连接注册）")
        return
    for item in entries:
        name = item.get("name", "?")
        try:
            target = target_from_url(item.get("url", ""))
            location = target.database if target.dialect == "sqlite" else \
                f"{target.host or '-'}:{target.port or '-'}" + (f"/{target.database}" if target.database else "")
            extras = [f"{key}={item[key]}" for key in ("username_env", "password_env") if item.get(key)]
            if item.get("aliases"):
                extras.append("aliases=" + ",".join(item["aliases"]))
            if item.get("allowed_databases"):
                extras.append("allowed_databases=" + ",".join(item["allowed_databases"]))
            if item.get("max_rows"):
                extras.append(f"max_rows={item['max_rows']}")
            if item.get("timeout_seconds"):
                extras.append(f"timeout_seconds={item['timeout_seconds']}")
            print(f"  {name:<24} {target.dialect:<12} {location:<40} {'  '.join(extras)}")
        except Exception as error:
            print(f"  {name:<24} [URL 无法解析：{error}]")
    report_server_env(entries)


def command_test(args):
    file = connections_file(args)
    entries = read_entries(file)
    match = [e for e in entries if e.get("name") == args.name]
    if not match:
        raise SystemExit(f"连接 {args.name} 不存在")
    target = target_from_url(match[0].get("url", ""))
    print(f"目标：类型={target.dialect} 主机={target.host or '-'} 端口={target.port or '-'} 库/文件={target.database or '-'}")
    own = {match[0][key] for key in CREDENTIAL_KEYS if match[0].get(key)}
    patches = {name: "validation" for name in referenced_env_vars(entries) if name not in own}
    payload = json.dumps({"connections": entries}, ensure_ascii=False, indent=2)
    with tempfile.TemporaryDirectory() as folder:
        candidate = Path(folder) / "connections.json"
        candidate.write_text(payload, encoding="utf-8")
        try:
            with patch.dict(os.environ, patches):
                loaded = load_connections(candidate)
        except Rejected as error:
            raise SystemExit(f"配置校验未通过：{error}\n（连接验证要求目标连接的凭据在当前环境提供真实值）") from None
    print("配置校验：通过")
    if not args.connect:
        return
    config = next(e for e in loaded if e.name == args.name)
    from readonly_mcp.backend import Backend
    try:
        engine = Backend(config).engine()
        with engine.connect() as conn:
            result = Backend(config).verify_account(conn)
    except Exception as error:
        kind = type(getattr(error, "orig", error)).__name__
        raise SystemExit(f"连接验证失败（{kind}）：请确认当前环境提供目标连接的真实凭据且网络/文件可访问") from None
    if config.target.dialect in ("mysql", "mariadb") and result:
        print("账号权限核验（SHOW GRANTS）：")
        for row in result:
            print("  " + "; ".join(str(v) for v in row.values()))
    print("连接验证：通过（服务将以同样方式建立只读连接）。")


def build_parser():
    parser = argparse.ArgumentParser(prog="manage_connections.py", description="数据库只读 MCP 的统一连接注册入口")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", help="连接文件路径（默认 DB_MCP_CONNECTIONS_FILE 或脚本同级 connections.json）")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list", parents=[common], help="列出已注册连接")
    add = sub.add_parser("add", parents=[common], help="注册连接（六种数据库统一入口）")
    add.add_argument("--type", required=True, choices=DIALECTS)
    add.add_argument("--name", required=True, help="连接名 [A-Za-z0-9_.-]+")
    add.add_argument("--url", help="完整连接地址（不含账号密码）")
    add.add_argument("--host")
    add.add_argument("--port", type=int)
    add.add_argument("--database", help="数据库名；mysql/mariadb 跨库连接不传")
    add.add_argument("--service-name", help="Oracle service name")
    add.add_argument("--db-file", help="sqlite 数据库文件（必须已存在）")
    add.add_argument("--username-env", help="存放只读账号的环境变量名")
    add.add_argument("--password-env", help="存放密码的环境变量名")
    add.add_argument("--alias", action="append", default=[], help="连接别名，可重复")
    add.add_argument("--max-rows", type=int, help="默认 1000，上限 10000")
    add.add_argument("--timeout-seconds", type=int, help="默认 15，上限 60")
    add.add_argument("--allowed-databases", help="跨库连接允许的库，逗号分隔（仅 mysql/mariadb 且未固定库）")
    add.add_argument("--replace", action="store_true", help="覆盖同名连接")
    remove = sub.add_parser("remove", parents=[common], help="移除连接")
    remove.add_argument("--name", required=True)
    test = sub.add_parser("test", parents=[common], help="校验连接；--connect 真实只读连接验证")
    test.add_argument("--name", required=True)
    test.add_argument("--connect", action="store_true")
    sub.add_parser("restore", parents=[common], help="从最近一次成功写入恢复连接文件")
    return parser


def main():
    args = build_parser().parse_args()
    {"add": command_add, "remove": command_remove, "restore": command_restore,
     "list": command_list, "test": command_test}[args.command](args)


if __name__ == "__main__":
    main()
