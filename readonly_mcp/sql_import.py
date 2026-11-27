"""Create a database from a large MySQL SQL dump and import it in parallel."""
from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from filelock import FileLock, Timeout
from sqlalchemy import text

from .config import Rejected
from .mirror import (ROOT, admin_session, admin_url, cleanup_account, client_args,
                     close_account_sessions, save_state, scoped_account)

MAX_OUTPUT_BYTES = 64 * 1024
DEFAULT_THREADS = 4
DEFAULT_CHUNK_MB = 64
MAX_CHUNK_MB = 1024
DROP_CONFIRM_BYTES = 1024 * 1024 * 1024
IMPORT_REFERENCE_TASK = "import-benchmark"
IMPORT_REFERENCE_BYTES = 6_615_558_946
IMPORT_REFERENCE_SECONDS = 511.593
IMPORT_REFERENCE_BYTES_PER_SECOND = IMPORT_REFERENCE_BYTES / IMPORT_REFERENCE_SECONDS
IMPORT_REFERENCE_LABEL = (
    f"参考任务 {IMPORT_REFERENCE_TASK}：{IMPORT_REFERENCE_BYTES / 1_000_000_000:.1f} GB / "
    f"{IMPORT_REFERENCE_SECONDS:.1f} 秒（约 {IMPORT_REFERENCE_BYTES_PER_SECOND / 1_048_576:.1f} MiB/s）"
)
COPY_BUFFER_BYTES = 8 * 1024 * 1024
JOB_SETUP = (
    b"SET NAMES utf8mb4;\n"
    b"SET @DBM_OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;\n"
    b"SET @DBM_OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;\n"
)


@dataclass(frozen=True)
class FileRange:
    start: int
    end: int

    @property
    def size(self):
        return self.end - self.start


@dataclass
class DumpBlock:
    kind: str
    name: str
    start: int
    end: int = 0
    inserts: list[FileRange] = field(default_factory=list)
    temporary_view: bool = False

    @property
    def table(self):
        return self.kind == "table"

    @property
    def prefix_end(self):
        return self.inserts[0].start if self.inserts else self.end

    @property
    def suffix_start(self):
        return self.inserts[-1].end if self.inserts else self.end


@dataclass(frozen=True)
class DumpPlan:
    path: Path
    size: int
    prelude: FileRange | None
    blocks: tuple[DumpBlock, ...]

    @property
    def table_blocks(self):
        return tuple(block for block in self.blocks if block.table)

    @property
    def auxiliary_blocks(self):
        return tuple(block for block in self.blocks if not block.table)

    @property
    def parallel(self):
        return bool(self.table_blocks)


@dataclass(frozen=True)
class ImportJob:
    label: str
    ranges: tuple[FileRange, ...]

    @property
    def size(self):
        return sum(item.size for item in self.ranges)


@dataclass(frozen=True)
class ImportJobs:
    setup: tuple[ImportJob, ...]
    data: tuple[ImportJob, ...]
    finish: ImportJob | None
    auxiliary: ImportJob | None

    @property
    def total_jobs(self):
        return len(self.setup) + len(self.data) + sum(
            item is not None for item in (self.finish, self.auxiliary)
        )


@dataclass(frozen=True)
class ObjectCounts:
    tables: int
    views: int
    routines: int
    triggers: int


STRUCTURE_MARKER = re.compile(
    r"^-- (?:Dumping structure for|Table structure for|Temporary view structure for) "
    r"(?:(table|view|procedure|function|trigger|event|routines|events)\s+)?(.+)$",
    re.I,
)


def _validate_database_name(name, label):
    if not isinstance(name, str) or not name or name != name.strip() or name.endswith(" "):
        raise Rejected(f"{label}不能为空或带首尾空格")
    if len(name.encode("utf-8")) > 64:
        raise Rejected(f"{label}超过 MySQL 64 字节上限")
    if any(char in name for char in ("/", "\\", ".", "`", "\x00")):
        raise Rejected(f"{label}不能包含斜杠、反斜杠、点、反引号或 NUL")
    if any(ord(char) < 32 for char in name):
        raise Rejected(f"{label}不能包含控制字符")
    return name


def database_name(path_value=None, override=None):
    if override is not None:
        return _validate_database_name(override, "数据库名")
    if path_value is None:
        raise Rejected("需要 sql_path 或 database_name")
    path = Path(path_value)
    if not path.is_absolute():
        raise Rejected("sql_path 必须是绝对路径")
    if path.suffix.lower() != ".sql":
        raise Rejected("sql_path 必须指向 .sql 文件")
    return _validate_database_name(path.name[:-4], "SQL 文件名")


def _marker(line):
    text_line = line[:2048].decode("utf-8", errors="replace").rstrip("\r\n")
    match = STRUCTURE_MARKER.match(text_line)
    if not match:
        return None
    kind = (match.group(1) or "table").lower()
    raw_name = match.group(2).strip()
    if kind in {"routines", "events"}:
        return kind, raw_name.strip("'`")
    if raw_name.startswith("`") and raw_name.endswith("`") and raw_name.count(".") == 0:
        name = raw_name.strip("`")
    else:
        name = raw_name.rsplit(".", 1)[-1].strip("`")
    if not name:
        raise Rejected("SQL dump 中存在无法识别对象名的结构标记")
    return kind, name


def scan_dump(path_value):
    path = Path(path_value)
    if not path.is_file():
        raise Rejected("sql_path 文件不存在")
    size = path.stat().st_size
    blocks = []
    current = None
    insert_start = None
    offset = 0
    with path.open("rb", buffering=COPY_BUFFER_BYTES) as source:
        for line in source:
            marker = _marker(line) if line.startswith(b"-- ") else None
            if marker is not None:
                if insert_start is not None:
                    raise Rejected("SQL dump 的结构标记出现在未结束的 INSERT 语句中")
                if current is not None:
                    current.end = offset
                    blocks.append(current)
                current = DumpBlock(kind=marker[0], name=marker[1], start=offset)
            elif current is not None and current.kind == "view" and line.startswith(
                b"-- Creating temporary table to overcome VIEW dependency errors"
            ):
                current.temporary_view = True
            elif current is not None and current.table:
                if line.startswith(b"INSERT INTO "):
                    if insert_start is not None:
                        raise Rejected("SQL dump 存在未闭合就重新开始的 INSERT 语句")
                    insert_start = offset
                if insert_start is not None and line.rstrip().endswith(b";"):
                    current.inserts.append(FileRange(insert_start, offset + len(line)))
                    insert_start = None
            offset += len(line)
    if insert_start is not None:
        raise Rejected("SQL dump 的 INSERT 语句在文件结束前未闭合")
    if current is not None:
        current.end = offset
        blocks.append(current)
    table_names = [block.name for block in blocks if block.table]
    if len(table_names) != len(set(table_names)):
        raise Rejected("SQL dump 包含重复的表结构标记")
    prelude = FileRange(0, blocks[0].start) if blocks and blocks[0].start else None
    return DumpPlan(path=path, size=size, prelude=prelude, blocks=tuple(blocks))


def _group_inserts(block, chunk_bytes):
    groups = []
    group_start = None
    group_end = None
    for item in block.inserts:
        if group_start is None:
            group_start, group_end = item.start, item.end
            continue
        if group_end - group_start >= chunk_bytes:
            groups.append(FileRange(group_start, group_end))
            group_start, group_end = item.start, item.end
        elif item.end - group_start > chunk_bytes:
            groups.append(FileRange(group_start, group_end))
            group_start, group_end = item.start, item.end
        else:
            group_end = item.end
    if group_start is not None:
        groups.append(FileRange(group_start, group_end))
    return groups


def build_jobs(plan, chunk_mb=DEFAULT_CHUNK_MB):
    if not isinstance(chunk_mb, int) or isinstance(chunk_mb, bool) or not 1 <= chunk_mb <= MAX_CHUNK_MB:
        raise Rejected(f"chunk_mb 必须在 1..{MAX_CHUNK_MB}")
    if not plan.parallel:
        return ImportJobs(
            setup=(ImportJob("sequential", (FileRange(0, plan.size),)),),
            data=(), finish=None, auxiliary=None,
        )
    suffix_ranges = []
    data = []
    setup = []
    chunk_bytes = chunk_mb * 1024 * 1024
    for block in plan.table_blocks:
        setup.append(ImportJob(
            f"table:{block.name}:setup",
            (FileRange(block.start, block.prefix_end),),
        ))
        if block.inserts and block.suffix_start < block.end:
            suffix_ranges.append(FileRange(block.suffix_start, block.end))
        groups = _group_inserts(block, chunk_bytes)
        for index, item in enumerate(groups, 1):
            data.append(ImportJob(f"table:{block.name}:{index}", (item,)))
    for block in plan.auxiliary_blocks:
        if block.kind == "view" and block.temporary_view:
            setup.append(ImportJob(
                f"view:{block.name}:temporary", (FileRange(block.start, block.end),),
            ))
    remaining_auxiliary = tuple(
        block for block in plan.auxiliary_blocks
        if not (block.kind == "view" and block.temporary_view)
    )
    return ImportJobs(
        setup=tuple(setup),
        data=tuple(data),
        finish=ImportJob("finish", tuple(suffix_ranges)) if suffix_ranges else None,
        auxiliary=(
            ImportJob("auxiliary", tuple(FileRange(block.start, block.end) for block in remaining_auxiliary))
            if remaining_auxiliary else None
        ),
    )


def _client_environment(url, login_file):
    return {
        **os.environ,
        "MYSQL_PWD": url.password or "",
        "MYSQL_TEST_LOGIN_FILE": str(login_file),
    }


def _error_code(errors):
    errors.seek(0)
    text_errors = errors.read(MAX_OUTPUT_BYTES).decode("utf-8", errors="replace")
    match = re.search(r"ERROR (\d+)", text_errors)
    return match.group(1) if match else None


def run_ranges(label, url, timeout, path, ranges, capture_output=False):
    if not ranges:
        return {"output": "", "truncated": False}
    args = client_args("mysql", url) + [
        "--binary-mode=1", "--local-infile=0", "--batch", "--database=" + url.database,
    ]
    with tempfile.TemporaryDirectory(prefix="dbm-import-client-") as options:
        login_file = Path(options) / "no-login.cnf"
        env = _client_environment(url, login_file)
        output = tempfile.TemporaryFile() if capture_output else subprocess.DEVNULL
        errors = tempfile.TemporaryFile()
        creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        try:
            process = subprocess.Popen(
                args, stdin=subprocess.PIPE, stdout=output, stderr=errors, env=env,
                creationflags=creationflags,
            )
        except OSError as error:
            errors.close()
            if capture_output:
                output.close()
            raise Rejected(f"{label} 启动 MySQL 客户端失败：{type(error).__name__}") from None
        feeder_error = []

        def feed():
            try:
                process.stdin.write(JOB_SETUP)
                with path.open("rb", buffering=COPY_BUFFER_BYTES) as source:
                    for item in ranges:
                        if item.size:
                            source.seek(item.start)
                            remaining = item.size
                            while remaining:
                                chunk = source.read(min(COPY_BUFFER_BYTES, remaining))
                                if not chunk:
                                    raise OSError("SQL 文件在导入期间缩短")
                                process.stdin.write(chunk)
                                remaining -= len(chunk)
                process.stdin.close()
            except (BrokenPipeError, ValueError):
                pass
            except Exception as error:
                feeder_error.append(type(error).__name__)
                try:
                    process.stdin.close()
                except OSError:
                    pass

        feeder = threading.Thread(target=feed, daemon=True)
        feeder.start()
        timed_out = False
        try:
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                process.kill()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    pass
        finally:
            feeder.join(timeout=10)
        if feeder.is_alive():
            try:
                process.stdin.close()
            except OSError:
                pass
            feeder.join(timeout=2)
        if timed_out:
            errors.close()
            if capture_output:
                output.close()
            raise Rejected(f"{label} 超过 {timeout} 秒；已终止本次 MySQL 客户端")
        if process.returncode or feeder_error:
            code = _error_code(errors)
            errors.close()
            if capture_output:
                output.close()
            detail = f"，错误码 {code}" if code else ""
            raise Rejected(f"{label} 导入失败（退出码 {process.returncode}{detail}）")
        errors.close()
        if not capture_output:
            return {"output": "", "truncated": False}
        size = output.tell()
        output.seek(0)
        content = output.read(MAX_OUTPUT_BYTES).decode("utf-8", errors="replace")
        output.close()
        return {"output": content, "truncated": size > MAX_OUTPUT_BYTES}


def _run_job(job, url, timeout, path, prelude):
    ranges = ((prelude,) if prelude else ()) + job.ranges
    return run_ranges(job.label, url, timeout, path, ranges)


def run_pipeline(plan, jobs, scoped_url, timeout, threads, progress=None, task=None):
    started = time.monotonic()
    phases = []
    if progress:
        progress(f"开始结构导入 jobs={len(jobs.setup)}")
    for job in jobs.setup:
        _run_job(job, scoped_url, timeout, plan.path, plan.prelude)
    if jobs.setup:
        phases.append({
            "phase": "setup", "jobs": len(jobs.setup),
            "bytes": sum(job.size for job in jobs.setup),
        })
        print(f"db_sql_import setup 完成：{len(jobs.setup)} 个任务", file=sys.stderr, flush=True)
        if progress:
            progress(f"结构导入完成 jobs={len(jobs.setup)}")
    setup_bytes = sum(job.size for job in jobs.setup)
    if task is not None:
        task.note_progress(
            completed=0, total=len(jobs.data),
            bytes_done=setup_bytes, bytes_total=plan.size,
        )
    completed = 0
    bytes_completed = 0
    first_error = None
    if jobs.data:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = {
                executor.submit(_run_job, job, scoped_url, timeout, plan.path, plan.prelude): job
                for job in jobs.data
            }
            for future in as_completed(futures):
                job = futures[future]
                try:
                    future.result()
                except Exception as error:
                    if first_error is None:
                        first_error = error
                else:
                    completed += 1
                    bytes_completed += job.size
                    if completed == len(jobs.data) or completed % max(1, len(jobs.data) // 10) == 0:
                        print(
                            f"db_sql_import 数据导入 {completed}/{len(jobs.data)} 个任务，"
                            f"{bytes_completed} 字节",
                            file=sys.stderr, flush=True,
                        )
                        if progress:
                            progress(
                                f"数据导入 {completed}/{len(jobs.data)} 个任务，{bytes_completed} 字节"
                            )
                        if task is not None:
                            task.note_progress(
                                completed=completed, total=len(jobs.data),
                                bytes_done=setup_bytes + bytes_completed,
                                bytes_total=plan.size,
                            )
        phases.append({
            "phase": "data", "jobs": len(jobs.data), "completed": completed,
            "bytes": bytes_completed,
        })
    if first_error is not None:
        raise first_error
    for job in (jobs.finish, jobs.auxiliary):
        if job:
            if progress:
                progress(f"执行 {job.label} 任务")
            _run_job(job, scoped_url, timeout, plan.path, plan.prelude)
            phases.append({"phase": job.label, "jobs": 1, "bytes": job.size})
            print(f"db_sql_import {job.label} 完成", file=sys.stderr, flush=True)
    if task is not None:
        task.note_progress(
            completed=completed, total=len(jobs.data),
            bytes_done=plan.size, bytes_total=plan.size,
        )
    return {
        "durationSeconds": round(time.monotonic() - started, 3),
        "phases": phases,
        "dataJobs": len(jobs.data),
        "completedDataJobs": completed,
    }


def _expected_objects(plan):
    tables = {block.name for block in plan.table_blocks}
    views = {block.name for block in plan.blocks if block.kind == "view"}
    routines = {block.name for block in plan.blocks if block.kind in {"procedure", "function"}}
    triggers = {block.name for block in plan.blocks if block.kind == "trigger"}
    return tables, views, routines, triggers


def _count_objects(conn, database):
    rows = conn.execute(text(
        "SELECT TABLE_TYPE, COUNT(*) FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=:db GROUP BY TABLE_TYPE"
    ), {"db": database}).all()
    kinds = {row[0]: int(row[1]) for row in rows}
    routines = int(conn.execute(text(
        "SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA=:db"
    ), {"db": database}).scalar() or 0)
    triggers = int(conn.execute(text(
        "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=:db"
    ), {"db": database}).scalar() or 0)
    return ObjectCounts(
        tables=kinds.get("BASE TABLE", 0),
        views=kinds.get("VIEW", 0),
        routines=routines,
        triggers=triggers,
    )


def _assert_expected_objects(conn, database, plan, counts):
    if not plan.parallel:
        return
    expected_tables, expected_views, expected_routines, expected_triggers = _expected_objects(plan)
    actual_tables = set(conn.execute(text(
        "SELECT TABLE_NAME FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=:db AND TABLE_TYPE='BASE TABLE'"
    ), {"db": database}).scalars())
    missing_tables = expected_tables - actual_tables
    if missing_tables:
        raise Rejected("导入完成但缺少表：" + ", ".join(sorted(missing_tables)[:10]))
    actual_views = set(conn.execute(text(
        "SELECT TABLE_NAME FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=:db AND TABLE_TYPE='VIEW'"
    ), {"db": database}).scalars())
    missing_views = expected_views - actual_views
    if missing_views:
        raise Rejected("导入完成但缺少视图：" + ", ".join(sorted(missing_views)[:10]))
    actual_routines = set(conn.execute(text(
        "SELECT ROUTINE_NAME FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA=:db"
    ), {"db": database}).scalars())
    missing_routines = expected_routines - actual_routines
    if missing_routines:
        raise Rejected("导入完成但缺少函数/过程：" + ", ".join(sorted(missing_routines)[:10]))
    actual_triggers = set(conn.execute(text(
        "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=:db"
    ), {"db": database}).scalars())
    missing_triggers = expected_triggers - actual_triggers
    if missing_triggers:
        raise Rejected("导入完成但缺少触发器：" + ", ".join(sorted(missing_triggers)[:10]))
    if counts.tables != len(expected_tables) or counts.views != len(expected_views):
        raise Rejected(
            f"对象数量不一致：期望表 {len(expected_tables)}、视图 {len(expected_views)}，"
            f"实际表 {counts.tables}、视图 {counts.views}"
        )


def _state_path():
    return Path(os.environ.get("DB_MCP_SQL_IMPORT_STATE_FILE", str(ROOT / "sql-import-state.json")))


def _load_state(path):
    if not path.exists():
        return {}
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise Rejected("SQL 导入状态文件损坏，未执行数据库操作：" + type(error).__name__) from None
    if not isinstance(state, dict):
        raise Rejected("SQL 导入状态文件格式无效，未执行数据库操作")
    return state


def _entry_key(instance, database):
    digest = hashlib.sha256((instance + "\x00" + database).encode("utf-8")).hexdigest()
    return "sqlimport-" + digest[:24]


def _server_instance(conn):
    try:
        instance = conn.exec_driver_sql("SELECT @@server_uuid").scalar()
    except Exception:
        instance = None
    if not instance:
        raise Rejected("SQL 导入仅支持可核验 server_uuid 的 MySQL 实例")
    return str(instance)


def _database_exists(conn, database):
    return bool(conn.execute(text(
        "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=:db"
    ), {"db": database}).scalar())


def _database_size(conn, database):
    value = conn.execute(text(
        "SELECT COALESCE(SUM(DATA_LENGTH + INDEX_LENGTH), 0) FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=:db"
    ), {"db": database}).scalar()
    return int(value or 0)


def _drop_database(conn, database):
    if _database_exists(conn, database):
        conn.exec_driver_sql("DROP DATABASE `" + database + "`")
    if _database_exists(conn, database):
        raise Rejected("数据库删除后仍存在")


def _cleanup_owned(conn, database, entry, state, state_path):
    errors = []
    try:
        _drop_database(conn, database)
    except Exception as error:
        errors.append("数据库清理失败：" + (str(error) if isinstance(error, Rejected) else type(error).__name__))
    if entry.get("account"):
        try:
            cleanup_account(conn, entry)
        except Exception as error:
            errors.append("临时账号清理失败：" + (str(error) if isinstance(error, Rejected) else type(error).__name__))
    entry["deleted"] = not _database_exists(conn, database)
    entry["status"] = "failed" if entry.get("status") == "importing" else "deleted"
    entry["finishedAt"] = datetime.now(timezone.utc).isoformat()
    save_state(state_path, state)
    return errors


def _verify_sql(scoped_url, sql, timeout):
    if not sql:
        return {"status": "not_run"}
    args = client_args("mysql", scoped_url) + [
        "--binary-mode=1", "--local-infile=0", "--batch", "--database=" + scoped_url.database,
    ]
    with tempfile.TemporaryDirectory(prefix="dbm-import-verify-") as options:
        env = _client_environment(scoped_url, Path(options) / "no-login.cnf")
        try:
            process = subprocess.run(
                args, input=sql.encode("utf-8"), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                env=env, timeout=timeout,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )
        except subprocess.TimeoutExpired:
            return {"status": "failed", "error": f"verify_sql 超过 {timeout} 秒"}
    if process.returncode:
        match = re.search(rb"ERROR (\d+)", process.stderr)
        return {
            "status": "failed",
            "error": "verify_sql 执行失败" + (f"，错误码 {match.group(1).decode()}" if match else ""),
        }
    output = process.stdout
    return {
        "status": "passed",
        "output": output[:MAX_OUTPUT_BYTES].decode("utf-8", errors="replace"),
        "truncated": len(output) > MAX_OUTPUT_BYTES,
    }


def _import_result(database, plan, jobs, pipeline, counts, verification, threads):
    return {
        "action": "import",
        "database": database,
        "sqlPath": str(plan.path),
        "bytes": plan.size,
        "mode": "parallel" if plan.parallel else "sequential",
        "threads": threads,
        "tables": len(plan.table_blocks),
        "auxiliaryBlocks": len(plan.auxiliary_blocks),
        "jobs": jobs.total_jobs,
        "objectCounts": counts.__dict__,
        "verificationStatus": verification["status"],
        "verification": verification if verification["status"] != "not_run" else None,
        **pipeline,
        "message": f"SQL 已导入新数据库 {database}；默认保留，删除需用户明确要求",
    }


def import_sql(config, arguments, job=None):
    sql_path = arguments.get("sql_path")
    if not isinstance(sql_path, str) or not sql_path:
        raise Rejected("action=import 需要 sql_path")
    database = database_name(sql_path, arguments.get("database_name"))
    if config.target.dialect != "mysql":
        raise Rejected("SQL 导入工具当前仅支持 MySQL")
    if Path(sql_path).suffix.lower() != ".sql" or not Path(sql_path).is_file():
        raise Rejected("sql_path 文件不存在或不是 .sql 文件")
    threads = arguments.get("threads", DEFAULT_THREADS)
    if not isinstance(threads, int) or isinstance(threads, bool) or not 1 <= threads <= 16:
        raise Rejected("threads 必须在 1..16")
    chunk_mb = arguments.get("chunk_mb", DEFAULT_CHUNK_MB)
    timeout = arguments.get("timeout_seconds", 3600)
    verify_sql = arguments.get("verify_sql", "")
    if not isinstance(verify_sql, str) or len(verify_sql) > 1_000_000:
        raise Rejected("verify_sql 不超过 1000000 字符")
    plan = scan_dump(sql_path)
    jobs = build_jobs(plan, chunk_mb)
    if job is not None:
        job.set_reference(
            plan.size, IMPORT_REFERENCE_BYTES_PER_SECOND, IMPORT_REFERENCE_LABEL,
        )
        job.note_progress(
            completed=0, total=len(jobs.data), bytes_done=0, bytes_total=plan.size,
        )
        job.progress(
            f"SQL 文件已扫描 bytes={plan.size} tables={len(plan.table_blocks)} "
            f"dataJobs={len(jobs.data)}"
        )
    url = admin_url(config)
    if url is None:
        raise Rejected("尚未配置 SQL 导入管理凭据，请先配置系统凭据存储或 MCP 环境变量")
    state_path = _state_path()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with FileLock(str(state_path) + ".lock", timeout=0):
            state = _load_state(state_path)
            with admin_session(config, url) as conn:
                instance = _server_instance(conn)
                if _database_exists(conn, database):
                    raise Rejected(f"数据库 {database} 已存在；本工具只创建新库，请先改名或清理旧库")
                source_id = _entry_key(instance, database)
                entry = {
                    "instance": instance,
                    "database": database,
                    "file": str(Path(sql_path).resolve()),
                    "status": "importing",
                    "createdAt": datetime.now(timezone.utc).isoformat(),
                }
                state[source_id] = entry
                save_state(state_path, state)
                created = False
                try:
                    conn.exec_driver_sql("CREATE DATABASE `" + database + "`")
                    created = True
                    with scoped_account(conn, database, state, source_id, state_path) as (user, password):
                        scoped_url = url.set(username=user, password=password, database=database)
                        pipeline = run_pipeline(
                            plan, jobs, scoped_url, timeout, threads,
                            progress=(job.progress if job is not None else None),
                            task=job,
                        )
                        if job is not None:
                            job.progress("导入数据已写入，核验对象与验证 SQL")
                        counts = _count_objects(conn, database)
                        _assert_expected_objects(conn, database, plan, counts)
                        verification = _verify_sql(scoped_url, verify_sql, timeout)
                    entry.update(
                        status="ready",
                        bytes=plan.size,
                        tables=counts.tables,
                        finishedAt=datetime.now(timezone.utc).isoformat(),
                    )
                    save_state(state_path, state)
                    return _import_result(
                        database, plan, jobs, pipeline, counts, verification, threads,
                    )
                except Exception as error:
                    cleanup_errors = _cleanup_owned(
                        conn, database, entry, state, state_path,
                    ) if created else []
                    if not created and not cleanup_errors:
                        entry["status"] = "failed"
                        entry["finishedAt"] = datetime.now(timezone.utc).isoformat()
                        save_state(state_path, state)
                    if cleanup_errors:
                        message = "；".join(cleanup_errors)
                        raise Rejected(f"SQL 导入失败且自动清理未完成：{message}") from None
                    if isinstance(error, Rejected):
                        raise
                    raise Rejected("SQL 导入失败：" + type(error).__name__) from None
    except Timeout:
        raise Rejected("另一个 SQL 导入或清理操作正在执行，请等待完成") from None


def status_sql(config, arguments):
    database = database_name(arguments.get("sql_path"), arguments.get("database_name"))
    if config.target.dialect != "mysql":
        raise Rejected("SQL 导入工具当前仅支持 MySQL")
    url = admin_url(config)
    if url is None:
        return {"configured": False, "database": database, "exists": None, "owned": False}
    state_path = _state_path()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with FileLock(str(state_path) + ".lock", timeout=0), admin_session(config, url) as conn:
            state = _load_state(state_path)
            instance = _server_instance(conn)
            key = _entry_key(instance, database)
            entry = state.get(key)
            exists = _database_exists(conn, database)
            return {
                "configured": True,
                "instance": instance,
                "database": database,
                "exists": exists,
                "owned": bool(entry and entry.get("instance") == instance and not entry.get("deleted")),
                "status": entry.get("status") if entry else None,
                "file": entry.get("file") if entry else None,
            }
    except Timeout:
        raise Rejected("另一个 SQL 导入或清理操作正在执行，请等待完成") from None


def drop_sql(config, arguments):
    database = database_name(arguments.get("sql_path"), arguments.get("database_name"))
    if config.target.dialect != "mysql":
        raise Rejected("SQL 导入工具当前仅支持 MySQL")
    url = admin_url(config)
    if url is None:
        raise Rejected("尚未配置 SQL 导入管理凭据")
    state_path = _state_path()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with FileLock(str(state_path) + ".lock", timeout=0), admin_session(config, url) as conn:
            state = _load_state(state_path)
            instance = _server_instance(conn)
            key = _entry_key(instance, database)
            entry = state.get(key)
            exists = _database_exists(conn, database)
            owned = bool(
                entry and entry.get("instance") == instance and not entry.get("deleted")
            )
            if exists and not owned:
                raise Rejected("数据库存在但不是 SQL 导入工具登记的资源；拒绝删除")
            confirm_large_drop = arguments.get("confirm_large_drop", False)
            if not isinstance(confirm_large_drop, bool):
                raise Rejected("confirm_large_drop 必须是布尔值")
            size = _database_size(conn, database) if exists and owned else 0
            if size > DROP_CONFIRM_BYTES and not confirm_large_drop:
                raise Rejected(
                    f"数据库 {database} 约 {size / DROP_CONFIRM_BYTES:.2f} GB，超过 1 GB；"
                    "删除前必须先与用户确认，确认后传 confirm_large_drop=true 重试"
                )
            if exists and owned and entry.get("account"):
                close_account_sessions(conn, entry["account"])
            errors = _cleanup_owned(conn, database, entry, state, state_path) if owned else []
            if errors:
                raise Rejected("；".join(errors))
            if entry:
                entry.update(status="deleted", deleted=True,
                             finishedAt=datetime.now(timezone.utc).isoformat())
                save_state(state_path, state)
            return {
                "action": "drop",
                "database": database,
                "exists": _database_exists(conn, database),
                "bytes": size,
                "message": "SQL 导入数据库已删除并核实不存在",
            }
    except Timeout:
        raise Rejected("另一个 SQL 导入或清理操作正在执行，请等待完成") from None


def run_sql_import(config, arguments, job=None):
    action = arguments["action"]
    if action == "import":
        return import_sql(config, arguments, job=job)
    if action == "status":
        return status_sql(config, arguments)
    if action == "drop":
        return drop_sql(config, arguments)
    raise Rejected("未知 SQL 导入 action")
