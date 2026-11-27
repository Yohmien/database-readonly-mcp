"""MCP-only mirror lifecycle; MySQL clients handle dump/restore and script execution."""
from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import signal
import shutil
import subprocess
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from pathlib import Path
from urllib.parse import quote

from filelock import FileLock, Timeout
import keyring
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

from .backend import Backend
from .config import Rejected, validate_url_options

ROOT = Path(__file__).resolve().parents[1]
MAX_OUTPUT_BYTES = 1_000_000
MAX_DUMP_BYTES = 128 * 1024 * 1024
MAX_DATASET_FILTER_LENGTH = 4000
MYSQLSH_THREADS = 4
MAX_TEST_OUTPUT_BYTES = 64 * 1024
MAX_TEST_COMMAND_ARGS = 64
MAX_TEST_ARG_LENGTH = 4096
WRITER_PRIVILEGES = {"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "INDEX",
                     "REFERENCES", "CREATE VIEW", "SHOW VIEW", "CREATE ROUTINE", "ALTER ROUTINE",
                     "EXECUTE", "TRIGGER", "EVENT", "CREATE TEMPORARY TABLES", "LOCK TABLES"}
TEST_ENV_KEYS = {
    "PATH", "PATHEXT", "SYSTEMDRIVE", "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP",
    "USERPROFILE", "HOMEDRIVE", "HOMEPATH", "APPDATA", "LOCALAPPDATA",
    "PROGRAMDATA", "PROGRAMFILES", "PROGRAMFILES(X86)", "JAVA_HOME",
    "MAVEN_HOME", "M2_HOME", "GRADLE_HOME", "NODE_HOME", "NODE_PATH",
    "PYTHONHOME", "PYTHONPATH", "PYTHONUTF8", "PYTHONIOENCODING", "TZ", "LANG",
    "LC_CTYPE", "LC_ALL", "TERM", "NO_COLOR"
}
SHELL_COMMANDS = {
    "cmd", "cmd.exe", "powershell", "powershell.exe", "pwsh", "pwsh.exe",
    "sh", "bash", "zsh", "fish"
}


def identity(config, server_uuid=None, canonical_database=None):
    target = config.target
    if target.dialect != "mysql" or not target.database:
        raise Rejected("镜像入口当前仅支持明确指定来源库的 MySQL")
    source = {"dialect": target.dialect, "instance": server_uuid, "database": canonical_database or target.database}
    if server_uuid is None:
        source.update(host=target.host, port=target.port)
    digest = hashlib.sha256(json.dumps(source, sort_keys=True).encode()).hexdigest()[:32]
    # No wildcard characters in the grant target.
    return digest, "dbm" + digest, source


def named_mirror(config, server_uuid, canonical_database, requested):
    """Resolve an explicit mirror name for single-mirror test runs.

    The override exists so a dedicated test database can be inspected or shared
    across invocations; it stays gated behind DB_MCP_MIRROR_ALLOW_NAMED=1 and is
    rejected whenever the name would collide with the source database itself.
    """
    if os.environ.get("DB_MCP_MIRROR_ALLOW_NAMED") != "1":
        raise Rejected("指定镜像库名仅在 MCP 配置 DB_MCP_MIRROR_ALLOW_NAMED=1 后可用；默认同来源唯一镜像不变")
    if not isinstance(requested, str) or not re.fullmatch(r"[0-9a-z_]{1,64}", requested):
        raise Rejected("mirror_name 仅允许 1-64 位小写字母、数字或下划线")
    source = {"dialect": config.target.dialect, "instance": server_uuid,
              "database": canonical_database or config.target.database}
    if requested == source["database"]:
        raise Rejected("mirror_name 不能与来源库同名")
    digest = hashlib.sha256(json.dumps({**source, "mirror": requested}, sort_keys=True).encode()).hexdigest()[:32]
    return digest, requested, source


def verify_writer(rows, database):
    found = False
    for row in rows:
        value = str(row[0])
        match = re.fullmatch(r"GRANT (.+?) ON (.+?) TO .+", value, re.I)
        if not match or "WITH GRANT OPTION" in value.upper():
            raise Rejected("镜像写账号存在角色、转授权或无法核验的权限")
        privileges = {x.strip().upper() for x in match[1].split(",")}
        scope = match[2]
        if scope == "*.*" and privileges == {"USAGE"}:
            continue
        if scope != f"`{database}`.*" or not (privileges <= WRITER_PRIVILEGES or privileges == {"ALL PRIVILEGES"}):
            raise Rejected("镜像写账号必须仅获当前镜像库权限；禁止来源库写权限、全局权限与其他库权限")
        found = True
    if not found:
        raise Rejected("镜像写账号缺少当前镜像库权限")


def admin_url(config):
    user = os.environ.get("DB_MCP_MIRROR_ADMIN_USER", "root")
    password = os.environ.get("DB_MCP_MIRROR_ADMIN_PASSWORD")
    if password is None:
        try:
            host = "localhost" if config.target.host in {"localhost", "127.0.0.1", "::1"} else config.target.host
            password = keyring.get_password("database-readonly-mcp", f"{user}@{host}:{config.target.port}")
        except keyring.errors.NoKeyringError:
            password = None
    if password is None:
        return None
    return validate_url_options(config.url.set(username=user, password=password, database=None))


@contextmanager
def admin_session(config, url):
    engine = create_engine(url, poolclass=NullPool, hide_parameters=True,
                           connect_args={"connect_timeout": config.timeout_seconds,
                                         "read_timeout": config.timeout_seconds,
                                         "write_timeout": config.timeout_seconds, "local_infile": False})
    try:
        with engine.connect() as conn:
            # Admin is used only for fixed lifecycle SQL, never caller SQL.
            if conn.exec_driver_sql("SELECT SUBSTRING_INDEX(CURRENT_USER(),'@',1)").scalar() != url.username:
                raise Rejected("镜像管理账号身份不匹配")
            conn.rollback()
            yield conn
    finally:
        engine.dispose()


def client_binary(name):
    configured = os.environ.get("DB_MCP_MYSQL_BIN")
    candidates = ([Path(configured) / (name + (".exe" if os.name == "nt" else ""))] if configured else [])
    discovered = shutil.which(name)
    if discovered:
        candidates.append(Path(discovered))
    if os.name == "nt":
        candidates.append(Path(os.environ.get("ProgramFiles", "C:/Program Files")) / "MySQL/MySQL Server 8.0/bin" / (name + ".exe"))
    for path in candidates:
        if path.is_file():
            return str(path)
    raise Rejected("未找到官方 MySQL 客户端，请在 MCP 配置 DB_MCP_MYSQL_BIN")


def client_args(name, url):
    args = [client_binary(name), "--no-defaults", "--protocol=TCP", "--host=" + (url.host or "localhost"),
            "--port=" + str(url.port or 3306), "--user=" + (url.username or ""), "--default-character-set=utf8mb4"]
    query = dict(url.query)
    # Never silently discard TLS or driver options during export/execute.
    mapping = {"ssl_ca": "ssl-ca", "ssl_cert": "ssl-cert", "ssl_key": "ssl-key"}
    if set(query) - {*mapping, "charset", "ssl_verify_cert", "ssl_verify_identity"}:
        raise Rejected("官方客户端暂不支持该连接选项")
    if query.get("charset", "utf8mb4").lower() != "utf8mb4":
        raise Rejected("镜像客户端仅支持 utf8mb4 连接字符集")
    for key, option in mapping.items():
        if key in query:
            args.append("--" + option + "=" + query[key])
    if str(query.get("ssl_verify_identity", "")).lower() in {"1", "true"}:
        args.append("--ssl-mode=VERIFY_IDENTITY")
    elif str(query.get("ssl_verify_cert", "")).lower() in {"1", "true"} or "ssl_ca" in query:
        args.append("--ssl-mode=VERIFY_CA")
    elif any(k in query for k in mapping):
        args.append("--ssl-mode=REQUIRED")
    return args


def run_client(args, url, timeout, sql=None, output_limit=MAX_OUTPUT_BYTES, *, dump_output=None):
    env = {**os.environ, "MYSQL_PWD": url.password or ""}
    with ExitStack() as stack:
        options = stack.enter_context(tempfile.TemporaryDirectory(prefix="dbm-client-"))
        output = dump_output if dump_output is not None else stack.enter_context(tempfile.TemporaryFile())
        errors = stack.enter_context(tempfile.TemporaryFile())
        sql_bytes = sql.encode("utf-8") if isinstance(sql, str) else None
        sql_file = None if isinstance(sql, str) else sql
        # MySQL 8.0 still reads .mylogin.cnf under --no-defaults and lacks --no-login-paths.
        env["MYSQL_TEST_LOGIN_FILE"] = str(Path(options) / "no-login.cnf")
        try:
            process = subprocess.run(args, input=sql_bytes, stdin=sql_file,
                                     stdout=output, stderr=errors, env=env, timeout=timeout,
                                     creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        except subprocess.TimeoutExpired:
            raise Rejected("MySQL 客户端超时；可能已有部分写入，请核验状态并经 MCP 清理镜像") from None
        if process.returncode:
            errors.seek(0)
            error = errors.read(4096).decode("utf-8", errors="replace")
            code = re.search(r"ERROR (\d+)", error)
            raise Rejected("MySQL 客户端失败，错误码 " + (code[1] if code else "未知") + "；可能已有部分写入")
        size = output.tell()
        output.seek(0)
        if dump_output is not None:
            return "", size > output_limit
        return output.read(output_limit).decode("utf-8", errors="replace"), size > output_limit


def redact_secret(value, secret):
    if not value or not secret:
        return value
    for candidate in {secret, quote(secret, safe="")}:
        value = value.replace(candidate, "***")
    return value


def validate_test_command(command, cwd, secret=None):
    if not isinstance(command, list) or not command or len(command) > MAX_TEST_COMMAND_ARGS:
        raise Rejected("run_test 需要非空 command 数组，最多 64 项")
    for argument in command:
        if (not isinstance(argument, str) or not argument or len(argument) > MAX_TEST_ARG_LENGTH
                or "\x00" in argument):
            raise Rejected("run_test 的每个命令参数必须非空、长度不超过 4096 且不含 NUL 字符")
    if Path(command[0]).name.lower() in SHELL_COMMANDS:
        raise Rejected("run_test 禁止通过 shell 启动；请直接传入测试脚本命令")
    if secret and any(secret in argument or quote(secret, safe="") in argument for argument in command):
        raise Rejected("run_test 禁止把测试账号密码放入命令参数")
    directory = Path(cwd)
    if not directory.is_absolute() or not directory.is_dir():
        raise Rejected("run_test 的 cwd 必须是存在的绝对目录")
    return directory


def test_environment(config, database, user, password):
    query = dict(config.url.query)
    if any(key in query for key in ("ssl", "ssl_mode", "tls", "ssl_ca", "ssl_cert", "ssl_key",
                                    "ssl_verify_cert", "ssl_verify_identity")):
        raise Rejected("run_test 暂不支持带 TLS 选项的镜像连接；未启动测试进程")
    host = config.target.host or "localhost"
    port = int(config.target.port or 3306)
    display_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
    jdbc_url = (
        f"jdbc:mysql://{display_host}:{port}/{database}"
        "?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull"
        "&useSSL=false&allowPublicKeyRetrieval=true"
    )
    env = {key: os.environ[key] for key in TEST_ENV_KEYS if key in os.environ}
    env.update({
        "DB_MCP_TEST_HOST": host,
        "DB_MCP_TEST_PORT": str(port),
        "DB_MCP_TEST_DATABASE": database,
        "DB_MCP_TEST_USER": user,
        "DB_MCP_TEST_PASSWORD": password,
        "DB_MCP_TEST_URL": f"mysql+pymysql://{display_host}:{port}/{database}",
        "DB_MCP_TEST_JDBC_URL": jdbc_url,
        "SPRING_DATASOURCE_URL": jdbc_url,
        "SPRING_DATASOURCE_USERNAME": user,
        "SPRING_DATASOURCE_PASSWORD": password,
        "SPRING_DATASOURCE_DRUID_MASTER_URL": jdbc_url,
        "SPRING_DATASOURCE_DRUID_MASTER_USERNAME": user,
        "SPRING_DATASOURCE_DRUID_MASTER_PASSWORD": password,
    })
    return env


def _resume_windows_process(process):
    import ctypes
    from ctypes import wintypes

    ntdll = ctypes.WinDLL("ntdll", use_last_error=True)
    ntdll.NtResumeProcess.argtypes = [wintypes.HANDLE]
    ntdll.NtResumeProcess.restype = ctypes.c_long
    status = ntdll.NtResumeProcess(wintypes.HANDLE(process._handle))
    if status != 0:
        raise OSError("NtResumeProcess failed")


def _abort_process(process):
    try:
        process.kill()
    except OSError:
        pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass


def _attach_windows_job(process):
    import ctypes
    from ctypes import wintypes

    class BasicLimitInformation(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", wintypes.LARGE_INTEGER),
            ("PerJobUserTimeLimit", wintypes.LARGE_INTEGER),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class IoCounters(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_ulonglong),
            ("WriteOperationCount", ctypes.c_ulonglong),
            ("OtherOperationCount", ctypes.c_ulonglong),
            ("ReadTransferCount", ctypes.c_ulonglong),
            ("WriteTransferCount", ctypes.c_ulonglong),
            ("OtherTransferCount", ctypes.c_ulonglong),
        ]

    class ExtendedLimitInformation(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", BasicLimitInformation),
            ("IoInfo", IoCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    kernel32.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
    kernel32.SetInformationJobObject.restype = wintypes.BOOL
    kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel32.TerminateJobObject.restype = wintypes.BOOL
    job = kernel32.CreateJobObjectW(None, None)
    if not job:
        raise OSError("CreateJobObjectW failed")
    info = ExtendedLimitInformation()
    info.BasicLimitInformation.LimitFlags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    if not kernel32.SetInformationJobObject(job, 9, ctypes.byref(info), ctypes.sizeof(info)):
        kernel32.CloseHandle(job)
        raise OSError("SetInformationJobObject failed")
    if not kernel32.AssignProcessToJobObject(job, wintypes.HANDLE(process._handle)):
        kernel32.CloseHandle(job)
        raise OSError("AssignProcessToJobObject failed")
    return job, kernel32


class _ProcessTree:
    def __init__(self, process):
        self.process = process
        self.job = None
        self.kernel32 = None
        self.pgid = None
        if os.name == "nt":
            try:
                self.job, self.kernel32 = _attach_windows_job(process)
            except OSError:
                raise Rejected("Windows 测试进程托管不可用；测试负载未执行") from None
            try:
                _resume_windows_process(process)
            except OSError:
                self.kernel32.TerminateJobObject(self.job, 1)
                self.kernel32.CloseHandle(self.job)
                self.job = None
                raise Rejected("Windows 测试进程恢复失败；测试负载未执行") from None
        else:
            # start_new_session=True makes the child its own process-group leader.
            self.pgid = process.pid

    def terminate(self):
        if self.job:
            return bool(self.kernel32.TerminateJobObject(self.job, 1))
        if self.pgid:
            try:
                os.killpg(self.pgid, signal.SIGKILL)
                return True
            except ProcessLookupError:
                return True
            except OSError:
                return False
        return False

    def close(self):
        ok = True
        if self.job:
            ok = bool(self.kernel32.TerminateJobObject(self.job, 1))
            if not self.kernel32.CloseHandle(self.job):
                ok = False
            self.job = None
        elif self.pgid:
            try:
                os.killpg(self.pgid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except OSError:
                ok = False
        return ok


class StreamingRedactor:
    def __init__(self, secret, limit, sink=None):
        patterns = set()
        if secret:
            patterns.add(secret.encode("utf-8"))
            patterns.add(quote(secret, safe="").encode("utf-8"))
        self.patterns = tuple(sorted(patterns, key=len, reverse=True))
        self.tail = max((len(pattern) for pattern in self.patterns), default=1) - 1
        self.pending = b""
        self.buffer = bytearray()
        self.total = 0
        self.limit = limit
        self.truncated = False
        self.sink = sink

    def feed(self, chunk):
        data = self.pending + chunk
        for pattern in self.patterns:
            data = data.replace(pattern, b"***")
        if self.tail:
            if len(data) > self.tail:
                self.pending = data[-self.tail:]
                data = data[:-self.tail]
            else:
                self.pending = data
                data = b""
        else:
            self.pending = b""
        self._append(data)

    def finish(self):
        data = self.pending
        self.pending = b""
        for pattern in self.patterns:
            data = data.replace(pattern, b"***")
        self._append(data)

    def _append(self, data):
        if not data:
            return
        if self.sink is not None:
            self.sink(data)
        self.total += len(data)
        remaining = self.limit - len(self.buffer)
        if remaining > 0:
            self.buffer.extend(data[:remaining])
        if self.total > self.limit:
            self.truncated = True

    def text(self):
        return bytes(self.buffer).decode("utf-8", errors="replace")


def _drain(pipe, redactor, errors):
    try:
        while True:
            chunk = pipe.read(8192)
            if not chunk:
                break
            redactor.feed(chunk)
    except Exception as error:
        errors.append(type(error).__name__)
    finally:
        try:
            pipe.close()
        except OSError:
            pass


def _wait_with_heartbeat(process, timeout, started):
    deadline = None if timeout is None else started + timeout
    while True:
        if deadline is None:
            wait = 5
        else:
            wait = max(0, min(5, deadline - time.monotonic()))
        try:
            process.wait(timeout=wait)
            return False
        except subprocess.TimeoutExpired:
            if deadline is not None and time.monotonic() >= deadline:
                return True


def run_child_process(command, cwd, env, secret, timeout, job=None):
    started = time.monotonic()
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    if os.name == "nt":
        creationflags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        creationflags |= getattr(subprocess, "CREATE_SUSPENDED", 0x4)
    try:
        process = subprocess.Popen(
            command,
            cwd=str(cwd),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            creationflags=creationflags if os.name == "nt" else 0,
            start_new_session=os.name != "nt",
        )
    except OSError as error:
        raise Rejected("测试进程启动失败：" + type(error).__name__) from None
    try:
        tree = _ProcessTree(process)
    except Rejected:
        _abort_process(process)
        for pipe in (process.stdout, process.stderr):
            try:
                pipe.close()
            except OSError:
                pass
        raise

    if job is not None:
        job.set_secret(secret)
        job.progress(f"测试进程已启动 pid={process.pid}")
    stdout = StreamingRedactor(
        secret, MAX_TEST_OUTPUT_BYTES,
        sink=(lambda data: job.stream("stdout", data)) if job is not None else None,
    )
    stderr = StreamingRedactor(
        secret, MAX_TEST_OUTPUT_BYTES,
        sink=(lambda data: job.stream("stderr", data)) if job is not None else None,
    )
    reader_errors = []
    readers = [
        threading.Thread(target=_drain, args=(process.stdout, stdout, reader_errors), daemon=True),
        threading.Thread(target=_drain, args=(process.stderr, stderr, reader_errors), daemon=True),
    ]
    for reader in readers:
        reader.start()

    timed_out = False
    cleanup_ok = True
    try:
        timed_out = _wait_with_heartbeat(process, timeout, started)
        if timed_out:
            if job is not None:
                job.progress("测试进程超时，正在清理进程树")
            cleanup_ok = tree.terminate() and cleanup_ok
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                _abort_process(process)
                cleanup_ok = False
    finally:
        cleanup_ok = tree.close() and cleanup_ok
        for reader in readers:
            reader.join(timeout=10)
        if any(reader.is_alive() for reader in readers):
            cleanup_ok = False
            for pipe in (process.stdout, process.stderr):
                try:
                    pipe.close()
                except OSError:
                    pass
            for reader in readers:
                reader.join(timeout=2)
        if not any(reader.is_alive() for reader in readers):
            stdout.finish()
            stderr.finish()
        else:
            cleanup_ok = False

    if reader_errors:
        cleanup_ok = False
    duration_ms = int((time.monotonic() - started) * 1000)
    if timed_out:
        status = "timed_out"
        message = "测试进程超时" + ("并已终止进程树" if cleanup_ok else "；进程树清理未确认")
    elif process.returncode == 0 and cleanup_ok:
        status = "passed"
        message = "测试进程退出码为 0"
    elif process.returncode == 0:
        status = "cleanup_failed"
        message = "测试进程退出码为 0，但进程树清理未确认"
    else:
        status = "failed"
        message = "测试进程非 0 退出" + ("" if cleanup_ok else "，且进程树清理未确认")
    if job is not None:
        job.progress(f"测试进程结束 status={status} exitCode={process.returncode}")
    return {
        "status": status,
        "exitCode": process.returncode,
        "timedOut": timed_out,
        "durationMs": duration_ms,
        "stdout": stdout.text(),
        "stderr": stderr.text(),
        "truncated": stdout.truncated or stderr.truncated,
        "cleanup": "ok" if cleanup_ok else "failed",
        "message": message,
    }


def execute(url, database, sql, timeout):
    args = client_args("mysql", url) + ["--binary-mode=1", "--local-infile=0", "--batch", "--database=" + database]
    output, truncated = run_client(args, url, timeout, sql)
    return {"output": output, "truncated": truncated, "message": "SQL 执行成功；DDL/客户端脚本不保证整体回滚"}


def source_tables(config, tables):
    # Existing readonly account verification remains the first source query.
    with Backend(config).session() as (conn, _):
        available = {row[0] for row in conn.execute(text("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=:db AND TABLE_TYPE='BASE TABLE'"), {"db": config.target.database})}
        selected = sorted(available if tables is None else set(tables))
        if set(selected) - available or any(t.startswith("-") for t in selected):
            raise Rejected("镜像表范围未知或名称不受官方导出客户端支持")
        return selected


def dataset_selection(config, arguments):
    tables = arguments.get("tables")
    if not isinstance(tables, list) or not tables:
        raise Rejected("测试数据集 tables 必须是非空表名数组")
    schema_mode = arguments.get("schema_mode", "selected")
    if schema_mode not in {"selected", "all"}:
        raise Rejected("schema_mode 仅支持 selected 或 all")
    requested = source_tables(config, tables)
    selected = sorted(source_tables(config, None) if schema_mode == "all" else requested)
    if not selected:
        raise Rejected("测试数据集至少需要一张基础表")
    prune = arguments.get("prune")
    supplement_tables = arguments.get("supplement_tables", [])
    if not isinstance(prune, dict) or not prune:
        raise Rejected("测试数据集必须提供非空 prune 过滤条件")
    if not isinstance(supplement_tables, list) or any(not isinstance(item, str) for item in supplement_tables):
        raise Rejected("supplement_tables 必须是表名数组")
    prune_tables = set(prune)
    supplement_tables = set(supplement_tables)
    if prune_tables & supplement_tables:
        raise Rejected("同一张表不能同时出现在 prune 和 supplement_tables")
    if not (prune_tables | supplement_tables) <= set(selected):
        raise Rejected("每张 tables 表必须且只能归入 prune 或 supplement_tables")
    if schema_mode == "selected" and prune_tables | supplement_tables != set(selected):
        raise Rejected("每张 tables 表必须且只能归入 prune 或 supplement_tables")
    prune = dict(prune)
    if schema_mode == "all":
        for table in set(selected) - prune_tables - supplement_tables:
            prune[table] = "1=0"
    filters = {}
    for table, condition in prune.items():
        if table not in selected:
            raise Rejected(f"prune 包含未选择表：{table}")
        if not isinstance(condition, str) or not condition.strip() or len(condition) > MAX_DATASET_FILTER_LENGTH:
            raise Rejected(f"prune.{table} 必须是非空且不超过 {MAX_DATASET_FILTER_LENGTH} 字符的条件")
        if any(token in condition.upper() for token in (";", "--", "/*", "*/", "#", "\x00")):
            raise Rejected(f"prune.{table} 只能包含单条过滤条件，禁止注释和语句分隔符")
        filters[f"{config.target.database}.{table}"] = condition.strip()
    return selected, filters, sorted(supplement_tables)


def dataset_options(filters, threads=4, bytes_per_chunk="8M"):
    if not isinstance(threads, int) or isinstance(threads, bool) or not 1 <= threads <= 16:
        raise Rejected("threads 必须在 1..16")
    if not isinstance(bytes_per_chunk, str) or not re.fullmatch(r"(?i)[1-9]\d{0,4}[KMG]", bytes_per_chunk):
        raise Rejected("bytes_per_chunk 必须使用 4M、8M、64M 等格式")
    return {
        "threads": threads,
        "bytesPerChunk": bytes_per_chunk.upper(),
        "consistent": False,
        "skipConsistencyChecks": True,
        "triggers": False,
        "showProgress": False,
        "compression": "none",
        "where": filters,
    }


def dataset_dump(config, selected, filters, threads, bytes_per_chunk, timeout, shell_home, dump_dir):
    try:
        binary = mysqlsh_binary()
        mysqlsh_call(
            binary,
            config.url,
            "dump_tables",
            [config.target.database, selected, str(dump_dir),
             dataset_options(filters, threads, bytes_per_chunk)],
            timeout,
            shell_home,
        )
        engine = "mysqlsh"
        fallback_reason = None
    except Rejected as error:
        if dump_dir.exists():
            if not dump_dir.is_dir():
                raise Rejected("MySQL Shell 导出失败后留下异常路径")
            shutil.rmtree(dump_dir)
        dump_dir.mkdir(parents=True)
        dataset_dump_mysqldump(config, selected, filters, threads, timeout, dump_dir)
        engine = "mysqldump"
        fallback_reason = str(error)
    files = [path for path in Path(dump_dir).rglob("*") if path.is_file()]
    size = sum(path.stat().st_size for path in files)
    if size > MAX_DUMP_BYTES:
        raise Rejected("测试数据集超过 128 MiB，未完成导出；请继续缩小 prune 范围")
    return {"bytes": size, "files": len(files), "engine": engine, "fallbackReason": fallback_reason}


def dataset_dump_mysqldump(config, selected, filters, threads, timeout, dump_dir):
    def dump_table(table):
        path = dump_dir / f"{table}.sql"
        args = client_args("mysqldump", config.url) + [
            "--single-transaction", "--quick", "--skip-lock-tables", "--no-tablespaces",
            "--set-gtid-purged=OFF", "--skip-triggers", "--skip-routines", "--skip-events",
            "--skip-add-locks", "--hex-blob", "--column-statistics=0",
        ]
        condition = filters.get(f"{config.target.database}.{table}")
        if condition:
            args.append("--where=" + condition)
        args.extend(["--", config.target.database, table])
        with path.open("wb") as output:
            _, truncated = run_client(args, config.url, timeout, output_limit=MAX_DUMP_BYTES,
                                      dump_output=output)
        if truncated:
            raise Rejected(f"表 {table} 导出超过 128 MiB，请缩小 prune 条件")
        return path.stat().st_size

    with ThreadPoolExecutor(max_workers=min(threads, len(selected))) as executor:
        sizes = list(executor.map(dump_table, selected))
    if sum(sizes) > MAX_DUMP_BYTES:
        raise Rejected("测试数据集超过 128 MiB，未完成导出；请继续缩小 prune 范围")


def combine_mysqldump_files(dump_dir, selected, output_path):
    with output_path.open("wb") as output:
        for table in selected:
            path = dump_dir / f"{table}.sql"
            if not path.is_file():
                raise Rejected(f"测试数据集缺少表导出文件：{table}")
            with path.open("rb") as source:
                shutil.copyfileobj(source, output, length=1024 * 1024)
                output.write(b"\n")
    return output_path


def prepare_test_dataset(config, arguments, job=None):
    selected, filters, supplement_tables = dataset_selection(config, arguments)
    if job is not None:
        job.progress(f"准备剪枝数据集 tables={len(selected)}")
    status = run_mirror(config, {"action": "status"})
    if not status.get("configured"):
        raise Rejected("尚未配置镜像管理凭据")
    if status["exists"]:
        raise Rejected("测试数据集要求没有现有镜像；请先完成或 drop 当前镜像")
    threads = arguments.get("threads", MYSQLSH_THREADS)
    bytes_per_chunk = arguments.get("bytes_per_chunk", "8M")
    timeout = arguments.get("timeout_seconds", 600)
    with tempfile.TemporaryDirectory(prefix="dbm-dataset-") as temporary:
        root = Path(temporary)
        shell_home = root / "shell"
        shell_home.mkdir()
        dump_dir = root / "dump"
        archive = dataset_dump(config, selected, filters, threads, bytes_per_chunk, timeout, shell_home, dump_dir)
        if job is not None:
            job.progress(f"剪枝导出完成 engine={archive['engine']}，准备载入镜像")
        load_file = None
        if archive["engine"] == "mysqldump":
            load_file = combine_mysqldump_files(dump_dir, selected, root / "load.sql")
        loaded = run_mirror(config, {
            "action": "dataset_load",
            "tables": selected,
            "dump_dir": str(dump_dir),
            "dataset_engine": archive["engine"],
            "load_file": str(load_file) if load_file else None,
            "threads": threads,
            "supplement_sql": arguments.get("supplement_sql", ""),
            "verify_sql": arguments.get("verify_sql", ""),
            "timeout_seconds": timeout,
        })
    return {
        **loaded,
        "action": "prepare",
        "prune": arguments["prune"],
        "supplementTables": supplement_tables,
        "schemaMode": arguments.get("schema_mode", "selected"),
        "archive": archive,
        "threads": threads,
        "message": "剪枝测试数据集已载入镜像；先用源库事实核对并在镜像隔离验证，测试完成必须 drop",
    }


def export_test_dataset(config, arguments, job=None):
    selected, filters, supplement_tables = dataset_selection(config, arguments)
    if job is not None:
        job.progress(f"导出剪枝数据集 tables={len(selected)}")
    output_value = arguments.get("output_dir")
    if not output_value or not Path(output_value).is_absolute():
        raise Rejected("export 需要绝对 output_dir")
    output_dir = Path(output_value)
    if output_dir.exists():
        raise Rejected("output_dir 已存在；为避免覆盖，请指定新目录")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    threads = arguments.get("threads", MYSQLSH_THREADS)
    bytes_per_chunk = arguments.get("bytes_per_chunk", "8M")
    timeout = arguments.get("timeout_seconds", 600)
    with tempfile.TemporaryDirectory(prefix=".dbm-export-", dir=output_dir.parent) as temporary:
        root = Path(temporary)
        staged = root / "bundle"
        staged.mkdir()
        shell_home = root / "shell"
        shell_home.mkdir()
        dump_dir = staged / "dump"
        archive = dataset_dump(config, selected, filters, threads, bytes_per_chunk, timeout, shell_home, dump_dir)
        if job is not None:
            job.progress(f"剪枝导出完成 engine={archive['engine']}，写入清单")
        manifest = {
            "format": 1,
            "sourceDatabase": config.target.database,
            "tables": selected,
            "prune": arguments["prune"],
            "supplementTables": supplement_tables,
            "schemaMode": arguments.get("schema_mode", "selected"),
            "threads": threads,
            "bytesPerChunk": bytes_per_chunk.upper(),
            "archive": archive,
            "dumpDir": "dump",
            "datasetEngine": archive["engine"],
            "supplementSqlFile": "supplement.sql" if arguments.get("supplement_sql") else None,
            "verifySql": arguments.get("verify_sql") or None,
        }
        (staged / "mcp-dataset.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n",
        )
        supplement_sql = arguments.get("supplement_sql")
        if supplement_sql:
            if len(supplement_sql) > 1_000_000:
                raise Rejected("supplement_sql 不超过 1000000 字符")
            (staged / "supplement.sql").write_text(
                supplement_sql.rstrip() + "\n", encoding="utf-8", newline="\n",
            )
        os.replace(staged, output_dir)
    return {
        "action": "export",
        "schemaMode": arguments.get("schema_mode", "selected"),
        "outputDir": str(output_dir),
        "source": {"database": config.target.database},
        "tables": selected,
        "prune": arguments["prune"],
        "supplementTables": supplement_tables,
        "archive": archive,
        "threads": threads,
        "message": "剪枝测试数据集已导出；可用 db_test_dataset action=load 从该目录恢复镜像",
    }


def load_test_dataset(config, arguments, job=None):
    input_value = arguments.get("input_dir")
    if not input_value or not Path(input_value).is_absolute():
        raise Rejected("load 需要绝对 input_dir")
    input_dir = Path(input_value).resolve()
    manifest_path = input_dir / "mcp-dataset.json"
    if not manifest_path.is_file():
        raise Rejected("input_dir 缺少 mcp-dataset.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("format") != 1 or manifest.get("sourceDatabase") != config.target.database:
        raise Rejected("导出数据集格式或来源数据库不匹配")
    tables = manifest.get("tables")
    prune = manifest.get("prune")
    supplement_tables = manifest.get("supplementTables", [])
    dataset_selection(config, {"tables": tables, "prune": prune,
                               "supplement_tables": supplement_tables,
                               "schema_mode": manifest.get("schemaMode", "selected")})
    dump_dir = (input_dir / manifest.get("dumpDir", "dump")).resolve()
    if input_dir not in dump_dir.parents or not dump_dir.is_dir():
        raise Rejected("导出数据集 dumpDir 不存在或越出 input_dir")
    status = run_mirror(config, {"action": "status"})
    if not status.get("configured"):
        raise Rejected("尚未配置镜像管理凭据")
    if status["exists"]:
        raise Rejected("测试数据集要求没有现有镜像；请先完成或 drop 当前镜像")
    supplement_sql = ""
    supplement_name = manifest.get("supplementSqlFile")
    if supplement_name:
        supplement_path = (input_dir / supplement_name).resolve()
        if input_dir not in supplement_path.parents or not supplement_path.is_file():
            raise Rejected("导出数据集 supplementSqlFile 不存在或越出 input_dir")
        supplement_sql = supplement_path.read_text(encoding="utf-8")
    engine = manifest.get("datasetEngine", "mysqlsh")
    with tempfile.TemporaryDirectory(prefix="dbm-dataset-load-") as temporary:
        if job is not None:
            job.progress(f"载入数据集 engine={engine} tables={len(tables)}")
        load_file = None
        if engine == "mysqldump":
            load_file = combine_mysqldump_files(dump_dir, tables, Path(temporary) / "load.sql")
        loaded = run_mirror(config, {
            "action": "dataset_load",
            "tables": tables,
            "dump_dir": str(dump_dir),
            "dataset_engine": engine,
            "load_file": str(load_file) if load_file else None,
            "threads": arguments.get("threads", manifest.get("threads", MYSQLSH_THREADS)),
            "supplement_sql": supplement_sql,
            "verify_sql": arguments.get("verify_sql") or manifest.get("verifySql") or "",
            "timeout_seconds": arguments.get("timeout_seconds", 600),
        })
    extension_results = []
    for extension in manifest.get("extensions", []):
        if job is not None:
            job.progress(f"重放增量 {extension.get('id', extension.get('path'))}")
        extension_dir = (input_dir / extension["path"]).resolve()
        if input_dir not in extension_dir.parents or not extension_dir.is_dir():
            raise Rejected("导出数据集 extension path 不存在或越出 input_dir")
        extension_dump = (extension_dir / extension.get("dumpDir", "dump")).resolve()
        if extension_dir not in extension_dump.parents or not extension_dump.is_dir():
            raise Rejected("导出数据集 extension dumpDir 不存在或越出扩展目录")
        extension_engine = extension.get("datasetEngine", "mysqlsh")
        extension_load_file = None
        if extension_engine == "mysqldump":
            with tempfile.TemporaryDirectory(prefix="dbm-dataset-extension-load-") as temporary:
                extension_load_file = combine_mysqldump_files(
                    extension_dump, extension["tables"], Path(temporary) / "load.sql",
                )
                extension_results.append(run_mirror(config, {
                    "action": "dataset_append",
                    "tables": extension["tables"],
                    "dump_dir": str(extension_dump),
                    "dataset_engine": extension_engine,
                    "load_file": str(extension_load_file),
                    "supplement_sql": extension.get("supplementSql", ""),
                    "verify_sql": extension.get("verifySql", ""),
                    "timeout_seconds": arguments.get("timeout_seconds", 600),
                }))
        else:
            extension_results.append(run_mirror(config, {
                "action": "dataset_append",
                "tables": extension["tables"],
                "dump_dir": str(extension_dump),
                "dataset_engine": extension_engine,
                "load_file": None,
                "supplement_sql": extension.get("supplementSql", ""),
                "verify_sql": extension.get("verifySql", ""),
                "threads": arguments.get("threads", manifest.get("threads", MYSQLSH_THREADS)),
                "timeout_seconds": arguments.get("timeout_seconds", 600),
            }))
    return {
        **loaded,
        "action": "load",
        "inputDir": str(input_dir),
        "prune": manifest["prune"],
        "supplementTables": manifest["supplementTables"],
        "schemaMode": manifest.get("schemaMode", "selected"),
        "extensions": extension_results,
        "message": "剪枝测试数据集已从导出目录载入镜像；先用源库事实核对并在镜像隔离验证，测试完成必须 drop",
    }


def extend_test_dataset(config, arguments, job=None):
    input_value = arguments.get("input_dir")
    if not input_value or not Path(input_value).is_absolute():
        raise Rejected("extend 需要绝对 input_dir")
    input_dir = Path(input_value).resolve()
    manifest_path = input_dir / "mcp-dataset.json"
    if not manifest_path.is_file():
        raise Rejected("input_dir 缺少 mcp-dataset.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("format") != 1 or manifest.get("sourceDatabase") != config.target.database:
        raise Rejected("导出数据集格式或来源数据库不匹配")
    selected, filters, supplement_tables = dataset_selection(config, arguments)
    if job is not None:
        job.progress(f"准备增量扩展 tables={len(selected)}")
    status = run_mirror(config, {"action": "status"})
    if not status.get("configured"):
        raise Rejected("尚未配置镜像管理凭据")
    if not status["exists"]:
        raise Rejected("extend 要求镜像已存在；请先 load 当前数据包")
    threads = arguments.get("threads", manifest.get("threads", MYSQLSH_THREADS))
    timeout = arguments.get("timeout_seconds", 600)
    extensions_dir = input_dir / "extensions"
    extensions_dir.mkdir(exist_ok=True)
    existing = sorted(
        int(path.name) for path in extensions_dir.iterdir()
        if path.is_dir() and re.fullmatch(r"\d{3}", path.name)
    )
    extension_id = f"{(existing[-1] + 1) if existing else 1:03d}"
    with tempfile.TemporaryDirectory(prefix=".dbm-extension-", dir=extensions_dir) as temporary:
        root = Path(temporary)
        bundle = root / "bundle"
        bundle.mkdir()
        shell_home = root / "shell"
        shell_home.mkdir()
        dump_dir = bundle / "dump"
        archive = dataset_dump(config, selected, filters, threads, "8M", timeout, shell_home, dump_dir)
        if job is not None:
            job.progress(f"增量导出完成 engine={archive['engine']}，载入镜像")
        load_file = None
        if archive["engine"] == "mysqldump":
            load_file = combine_mysqldump_files(dump_dir, selected, root / "load.sql")
        supplement_sql = arguments.get("supplement_sql", "")
        verify_sql = arguments.get("verify_sql", "")
        load_failure = None
        try:
            loaded = run_mirror(config, {
                "action": "dataset_append",
                "tables": selected,
                "dump_dir": str(dump_dir),
                "dataset_engine": archive["engine"],
                "load_file": str(load_file) if load_file else None,
                "supplement_sql": supplement_sql,
                "verify_sql": verify_sql,
                "threads": threads,
                "timeout_seconds": timeout,
            })
        except Exception as error:
            loaded = {}
            load_failure = str(error) if isinstance(error, Rejected) else type(error).__name__
        verification_status = loaded.get("verificationStatus", "not_run")
        if load_failure or verification_status == "failed":
            extension_status = "failed"
        elif verification_status == "passed":
            extension_status = "passed"
        else:
            extension_status = "loaded"
        extension = {
            "id": extension_id,
            "path": f"extensions/{extension_id}",
            "tables": selected,
            "prune": arguments["prune"],
            "supplementTables": supplement_tables,
            "dumpDir": "dump",
            "datasetEngine": archive["engine"],
            "archive": archive,
            "supplementSql": supplement_sql,
            "verifySql": verify_sql or None,
            "status": extension_status,
            "verificationStatus": verification_status,
            "failure": load_failure or loaded.get("verificationError"),
        }
        (bundle / "extension.json").write_text(
            json.dumps(extension, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8", newline="\n",
        )
        target = extensions_dir / extension_id
        os.replace(bundle, target)
    manifest.setdefault("extensions", []).append(extension)
    temporary_manifest = manifest_path.with_suffix(".json.tmp")
    temporary_manifest.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8", newline="\n",
    )
    os.replace(temporary_manifest, manifest_path)
    if load_failure:
        raise Rejected(
            f"增量轮次 {extension_id} 已记录，但镜像加载失败：{load_failure}"
        )
    if verification_status == "failed":
        message = (
            f"增量轮次 {extension_id} 已记录，但隔离验证失败；"
            "请回源库重新定位剪枝差异后执行下一轮 extend"
        )
    else:
        message = (
            "本轮剪枝缺口已追加到当前镜像和数据包；"
            "请立即隔离复验，仍失败时先回源库定位新的剪枝差异再执行下一轮 extend"
        )
    return {
        **loaded,
        "action": "extend",
        "inputDir": str(input_dir),
        "extensionId": extension_id,
        "extensionStatus": extension_status,
        "verificationStatus": verification_status,
        "tables": selected,
        "prune": arguments["prune"],
        "supplementTables": supplement_tables,
        "archive": archive,
        "message": message,
    }


def source_dump(config, selected, empty, timeout, dump_file):
    if not selected:
        return
    args = client_args("mysqldump", config.url) + ["--single-transaction", "--quick", "--skip-lock-tables",
            "--no-tablespaces", "--set-gtid-purged=OFF", "--skip-triggers", "--skip-routines", "--skip-events",
            "--skip-add-locks", "--hex-blob", "--column-statistics=0"]
    if empty:
        args.append("--no-data")
    args.extend(["--", config.target.database, *selected])
    try:
        _, truncated = run_client(args, config.url, timeout, output_limit=MAX_DUMP_BYTES,
                                  dump_output=dump_file)
    except Rejected as error:
        reason = str(error).split("；", 1)[0]
        raise Rejected(f"来源导出失败（{reason}），未开始重建；大库请缩小 tables 范围或使用 init_empty 导入必要样本") from None
    if truncated:
        raise Rejected("导出超过 128 MiB 上限，未开始重建；请缩小 tables 范围或使用 init_empty 导入必要样本")


def mysqlsh_binary():
    configured = os.environ.get("DB_MCP_MYSQLSH_BIN")
    if configured:
        if Path(configured).is_file():
            return configured
        raise Rejected("DB_MCP_MYSQLSH_BIN 指向的 MySQL Shell 不存在")
    discovered = shutil.which("mysqlsh")
    if discovered:
        return discovered
    bundled = ROOT / "vendor/mysql-shell/bin/mysqlsh.exe"
    if bundled.is_file():
        return str(bundled)
    raise Rejected("未找到 MySQL Shell，请设置 DB_MCP_MYSQLSH_BIN 或将 mysqlsh 加入 PATH")


def ensure_local_infile(conn):
    if conn.exec_driver_sql("SELECT @@GLOBAL.local_infile").scalar():
        return
    conn.exec_driver_sql("SET PERSIST local_infile=ON")
    if not conn.exec_driver_sql("SELECT @@GLOBAL.local_infile").scalar():
        raise Rejected("local_infile 开启后仍为 OFF，未开始镜像重建")


def mysqlsh_call(binary, url, method, arguments, timeout, config_home):
    connection = {"scheme": "mysql", "host": url.host or "localhost",
                  "port": url.port or 3306, "user": url.username}
    query = dict(url.query)
    tls = {"ssl_ca": "ssl-ca", "ssl_cert": "ssl-cert", "ssl_key": "ssl-key"}
    if set(query) - {*tls, "charset", "ssl_verify_cert", "ssl_verify_identity"}:
        raise Rejected("MySQL Shell 暂不支持该连接选项")
    if query.get("charset", "utf8mb4").lower() != "utf8mb4":
        raise Rejected("MySQL Shell 镜像仅支持 utf8mb4")
    for key, option in tls.items():
        if key in query:
            connection[option] = query[key]
    if str(query.get("ssl_verify_identity", "")).lower() in {"1", "true"}:
        connection["ssl-mode"] = "VERIFY_IDENTITY"
    elif str(query.get("ssl_verify_cert", "")).lower() in {"1", "true"} or "ssl_ca" in query:
        connection["ssl-mode"] = "VERIFY_CA"
    elif any(key in query for key in tls):
        connection["ssl-mode"] = "REQUIRED"
    env = {key: value for key, value in os.environ.items() if key.upper() in TEST_ENV_KEYS}
    if "PATH" in env:
        env["PATH"] = os.pathsep.join(
            entry for entry in env["PATH"].split(os.pathsep) if entry and Path(entry).is_dir()
        )
    env.update(MYSQLSH_USER_CONFIG_HOME=str(config_home), DB_MCP_SHELL_PASSWORD=url.password or "",
               DB_MCP_SHELL_CONNECTION=json.dumps(connection),
               DB_MCP_SHELL_ARGUMENTS=json.dumps(arguments))
    utility_call = f"util.{method}(*json.loads(os.environ['DB_MCP_SHELL_ARGUMENTS']))"
    script = (
        "import json, os\n"
        "connection = json.loads(os.environ['DB_MCP_SHELL_CONNECTION'])\n"
        "connection['password'] = os.environ.pop('DB_MCP_SHELL_PASSWORD')\n"
        "shell.connect(connection)\n"
        f"{utility_call}\n"
    )
    phase = "来源导出" if method == "dump_tables" else "镜像导入"
    guidance = "；未开始重建，可缩小 tables 范围" if method == "dump_tables" else "；请经 MCP 核验或清理镜像"
    with tempfile.TemporaryFile() as output:
        try:
            process = subprocess.run([binary, "--py", "--no-wizard", "--log-level=none", "-e", script],
                                     stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                                     env=env, timeout=timeout,
                                     creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        except subprocess.TimeoutExpired:
            raise Rejected(f"MySQL Shell {phase}超时{guidance}") from None
        if process.returncode:
            output.seek(0)
            # The utility can print SQL or data values; expose only diagnostic lines.
            errors = output.read(MAX_OUTPUT_BYTES).decode("utf-8", errors="replace")
            codes = ", ".join(sorted(set(re.findall(r"(?:MySQL Error |MYSQLSH |ERROR: )(\d+)", errors))))
            diagnostics = [
                line.strip()[:500]
                for line in errors.splitlines()
                if re.search(r"(?i)\b(error|denied|privilege|requires|role|failed)\b", line)
            ]
            detail = "；".join(diagnostics[-2:])
            suffix = f"；{detail}" if detail else ""
            raise Rejected(
                f"MySQL Shell {phase}失败（退出码 {process.returncode}，错误码 {codes or '未知'}）{guidance}{suffix}"
            )


def source_defaults(config):
    with Backend(config).session() as (conn, _):
        row = conn.execute(text("SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=:db"), {"db": config.target.database}).first()
        if row is None or any(not re.fullmatch(r"[a-zA-Z0-9_]+", v) for v in row):
            raise Rejected("来源库不存在或字符集定义无法核验")
        return row


def close_account_sessions(conn, account):
    sessions = list(conn.execute(text("SELECT ID FROM information_schema.PROCESSLIST WHERE USER=:u"), {"u": account["user"]}).scalars())
    for session in sessions:
        try:
            conn.exec_driver_sql("KILL CONNECTION " + str(int(session)))
        except Exception as error:
            if getattr(getattr(error, "orig", error), "args", (None,))[0] != 1094:
                raise


def cleanup_account(conn, entry):
    account = entry.get("account")
    if account:
        if not re.fullmatch(r"dbm[0-9a-f]{24}", account["user"]):
            raise Rejected("临时账号登记无效，拒绝删除")
        close_account_sessions(conn, account)
        conn.exec_driver_sql("DROP USER IF EXISTS %s@%s", (account["user"], account["host"]))
        if conn.execute(text("SELECT COUNT(*) FROM mysql.user WHERE User=:u AND Host=:h"), {"u": account["user"], "h": account["host"]}).scalar():
            raise Rejected("临时镜像账号清理后仍存在")
        entry.pop("account")


@contextmanager
def scoped_account(conn, database, state, source_id, state_path):
    version = str(conn.exec_driver_sql("SELECT VERSION()").scalar())
    if int(version.split(".")[0]) >= 8 and conn.exec_driver_sql("SELECT @@mandatory_roles").scalar():
        raise Rejected("实例配置了强制角色，无法保证测试账号仅限镜像库；未执行待测 SQL")
    password = secrets.token_urlsafe(32)
    entry = state[source_id]
    account = entry.get("account")
    if account:
        user, host = account["user"], account["host"]
        close_account_sessions(conn, account)
        conn.exec_driver_sql("ALTER USER %s@%s IDENTIFIED BY %s", (user, host, password))
    else:
        user = "dbm" + secrets.token_hex(12)
        host = conn.exec_driver_sql("SELECT SUBSTRING_INDEX(USER(),'@',-1)").scalar()
        if conn.execute(text("SELECT COUNT(*) FROM mysql.user WHERE User=:u"), {"u": user}).scalar():
            raise Rejected("临时测试账号发生命名冲突，未执行 SQL")
        account = {"user": user, "host": host}
        entry["account"] = account
        save_state(state_path, state)
        conn.exec_driver_sql("CREATE USER %s@%s IDENTIFIED BY %s", (user, host, password))
        conn.exec_driver_sql(f"GRANT ALL PRIVILEGES ON `{database}`.* TO %s@%s", (user, host))
    try:
        # Verify the actual grant, independently of the grant statement above.
        verify_writer(list(conn.exec_driver_sql("SHOW GRANTS FOR %s@%s", (user, host))), database)
        yield user, password
    finally:
        # Preserve the definer until its mirror objects have been dropped.
        close_account_sessions(conn, account)


def execute_scoped(conn, url, database, sql, timeout, state, source_id, state_path):
    with scoped_account(conn, database, state, source_id, state_path) as (user, password):
        return execute(url.set(username=user, password=password), database, sql, timeout)


def run_test_scoped(conn, config, database, command, cwd, timeout, state, source_id, state_path, job=None):
    with scoped_account(conn, database, state, source_id, state_path) as (user, password):
        validate_test_command(command, cwd, password)
        env = test_environment(config, database, user, password)
        if job is not None:
            job.set_secret(password)
            job.progress(f"镜像测试环境就绪 database={database}")
        return run_child_process(command, cwd, env, password, timeout, job=job)


def resolved_identity(conn, config, state):
    server_uuid = conn.exec_driver_sql("SELECT @@server_uuid").scalar()
    canonical = conn.execute(text("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=:db AND (BINARY SCHEMA_NAME=BINARY :db OR @@lower_case_table_names<>0)"), {"db": config.target.database}).scalar()
    if not server_uuid:
        raise Rejected("无法核验来源实例身份")
    if canonical is None:
        ignore_case = bool(conn.exec_driver_sql("SELECT @@lower_case_table_names").scalar())
        supplied = config.target.database
        candidates = [entry["source"]["database"] for entry in state.values()
                      if entry.get("source", {}).get("instance") == str(server_uuid)
                      and ((entry["source"]["database"].casefold() == supplied.casefold()) if ignore_case
                           else entry["source"]["database"] == supplied)]
        if len(candidates) != 1:
            raise Rejected("来源库不存在且无法唯一恢复已登记绑定；未删除镜像，不能确认清理成功")
        canonical = candidates[0]
    return identity(config, str(server_uuid), canonical)


def run_mirror(config, arguments, job=None):
    identity(config)  # Validate the supported source before resolving credentials.
    action = arguments["action"]
    timeout = arguments.get("timeout_seconds", 60)
    test_timeout = arguments.get("timeout_seconds")
    clone_engine = arguments.get("clone_engine", "mysqldump")
    if "clone_engine" in arguments and action != "clone":
        raise Rejected("clone_engine 仅用于 clone")
    if clone_engine == "mysqlsh" and not arguments.get("tables"):
        raise Rejected("MySQL Shell 并行克隆需要用 tables 显式指定测试所需的基础表")
    if "sql" in arguments and action != "execute" or "tables" in arguments and action not in {"clone", "init_empty", "dataset_load", "dataset_append"}:
        raise Rejected("sql 仅用于 execute；tables 仅用于 clone/init_empty/dataset_load/dataset_append")
    requested_name = arguments.get("mirror_name")
    if requested_name is not None and action == "status":
        raise Rejected("mirror_name 不用于 status；指定名称的镜像按 create/execute/run_test/drop 核验")
    if action == "execute" and (not arguments.get("sql", "").strip() or len(arguments["sql"]) > 1_000_000):
        raise Rejected("execute 需要非空且不超过 1000000 字符的 SQL")
    if action == "run_test":
        cwd = validate_test_command(arguments.get("command"), arguments.get("cwd", str(ROOT)))
    elif "command" in arguments or "cwd" in arguments:
        raise Rejected("command/cwd 仅用于 run_test")
    url = admin_url(config)
    if url is None:
        if action == "status":
            return {"sourceId": None, "database": None, "action": action, "configured": False, "exists": None, "message": "尚未配置镜像管理凭据；未核验来源实例身份"}
        raise Rejected("尚未配置镜像管理凭据，请先配置系统凭据存储或 MCP 环境变量")
    state_path = Path(os.environ.get("DB_MCP_MIRROR_STATE_FILE", str(ROOT / "mirror-state.json")))
    state_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with FileLock(str(state_path) + ".lock", timeout=0), ExitStack() as stack:
            state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
            with admin_session(config, url) as conn:
                if requested_name is None:
                    source_id, database, source = resolved_identity(conn, config, state)
                else:
                    server_uuid = conn.exec_driver_sql("SELECT @@server_uuid").scalar()
                    canonical = conn.execute(text("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=:db AND (BINARY SCHEMA_NAME=BINARY :db OR @@lower_case_table_names<>0)"), {"db": config.target.database}).scalar()
                    if not server_uuid or canonical is None:
                        raise Rejected("无法核验来源实例身份或来源库不存在")
                    source_id, database, source = named_mirror(config, str(server_uuid), canonical, requested_name)
                result = {"sourceId": source_id, "source": source, "database": database, "action": action, "configured": True}
                exists = bool(conn.execute(text("SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=:db"), {"db": database}).scalar())
                entry = state.get(source_id, {})
                owned = entry.get("source") == source
                result.update(exists=exists, owned=owned)
                if job is not None:
                    job.progress(f"镜像库={database} exists={exists} owned={owned}")
                if action == "status":
                    return result
                if exists and not owned:
                    raise Rejected("唯一镜像库名已被未登记资源占用；禁止接管或删除")
                if action == "drop":
                    if job is not None:
                        job.progress("删除镜像库并核验不存在")
                    if entry.get("account"):
                        close_account_sessions(conn, entry["account"])
                    if exists:
                        conn.exec_driver_sql(f"DROP DATABASE `{database}`")
                    if owned:
                        cleanup_account(conn, entry)
                    remaining = conn.execute(text("SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=:db"), {"db": database}).scalar()
                    if remaining:
                        raise Rejected("镜像库删除后仍存在，清理未完成")
                    # Keep only the source binding so missing-source cleanup remains verifiable.
                    state[source_id] = {"source": source, "deleted": True}
                    save_state(state_path, state)
                    return {**result, "exists": False, "message": "镜像库已删除并核实不存在"}
                if action in {"clear", "execute", "run_test", "dataset_append"} and not exists:
                    raise Rejected("镜像库不存在，请先 create 或初始化")
                if action == "execute":
                    if job is not None:
                        job.progress("执行镜像 SQL")
                    return {**result, **execute_scoped(conn, url, database, arguments["sql"], timeout, state, source_id, state_path)}
                if action == "dataset_append":
                    if job is not None:
                        job.progress("增量载入镜像数据集")
                    selected = source_tables(config, arguments.get("tables"))
                    dump_dir = Path(arguments["dump_dir"])
                    if not dump_dir.is_dir():
                        raise Rejected("增量数据集 dump_dir 不存在")
                    dataset_engine = arguments.get("dataset_engine", "mysqlsh")
                    threads = arguments.get("threads", MYSQLSH_THREADS)
                    if not isinstance(threads, int) or isinstance(threads, bool) or not 1 <= threads <= 16:
                        raise Rejected("threads 必须在 1..16")
                    if dataset_engine == "mysqlsh":
                        shell_home = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="dbm-shell-")))
                        ensure_local_infile(conn)
                        binary = mysqlsh_binary()
                        with scoped_account(conn, database, state, source_id, state_path) as (user, password):
                            mysqlsh_call(binary, url.set(username=user, password=password), "load_dump", [
                                str(dump_dir),
                                {"schema": database, "threads": threads, "showProgress": False},
                            ], timeout, shell_home)
                    elif dataset_engine == "mysqldump":
                        load_file = Path(arguments.get("load_file", ""))
                        if not load_file.is_file():
                            raise Rejected("增量数据集缺少 mysqldump 合并载入文件")
                        with load_file.open("rb") as sql_file:
                            execute_scoped(
                                conn, url, database, sql_file, timeout, state, source_id, state_path,
                            )
                    else:
                        raise Rejected("未知 dataset_engine")
                    supplement_output = None
                    if arguments.get("supplement_sql"):
                        supplement_output = execute_scoped(
                            conn, url, database, arguments["supplement_sql"], timeout,
                            state, source_id, state_path,
                        )
                    verification_output = None
                    verification_status = "not_run"
                    if arguments.get("verify_sql"):
                        try:
                            verification_output = execute_scoped(
                                conn, url, database, arguments["verify_sql"], timeout,
                                state, source_id, state_path,
                            )
                            verification_status = "passed"
                        except Exception as error:
                            verification_status = "failed"
                            result["verificationError"] = (
                                str(error) if isinstance(error, Rejected) else type(error).__name__
                            )
                    result.update(
                        cloneEngine=dataset_engine, datasetAppend=True, tables=selected,
                        verificationStatus=verification_status,
                    )
                    if supplement_output is not None:
                        result["supplement"] = supplement_output
                    if verification_output is not None:
                        result["verification"] = verification_output
                    return result
                if action == "run_test":
                    if job is not None:
                        job.progress("镜像与测试账号准备中")
                    return {**result, **run_test_scoped(conn, config, database, arguments["command"], cwd,
                                                         test_timeout, state, source_id, state_path, job=job)}
                if action == "clear":
                    if job is not None:
                        job.progress("清空镜像表数据")
                    tables = list(conn.execute(text("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=:db AND TABLE_TYPE='BASE TABLE'"), {"db": database}).scalars())
                    sql = "SET FOREIGN_KEY_CHECKS=0;\n" + "\n".join(f"TRUNCATE TABLE `{database}`.`{t.replace('`', '``')}`;" for t in tables) + "\nSET FOREIGN_KEY_CHECKS=1;"
                    return {**result, **execute_scoped(conn, url, database, sql, timeout, state, source_id, state_path)}
                if action == "create" and exists:
                    return {**result, "message": "已复用当前来源的唯一镜像库"}
                dump = None
                selected = []
                if action in {"init_empty", "clone", "dataset_load"}:
                    if job is not None:
                        job.progress(f"准备镜像 {action}：解析来源表")
                    selected = source_tables(config, arguments.get("tables"))
                    if action == "dataset_load":
                        shell_home = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="dbm-shell-")))
                        dump_dir = Path(arguments["dump_dir"])
                        if not dump_dir.is_dir():
                            raise Rejected("测试数据集 dump_dir 不存在，未开始重建")
                        dataset_engine = arguments.get("dataset_engine", "mysqlsh")
                        if dataset_engine == "mysqlsh":
                            ensure_local_infile(conn)
                            binary = mysqlsh_binary()
                        elif dataset_engine == "mysqldump":
                            load_file = Path(arguments.get("load_file", ""))
                            if not load_file.is_file():
                                raise Rejected("mysqldump 测试数据集缺少合并载入文件，未开始重建")
                        else:
                            raise Rejected("未知 dataset_engine")
                        threads = arguments.get("threads", MYSQLSH_THREADS)
                        if not isinstance(threads, int) or isinstance(threads, bool) or not 1 <= threads <= 16:
                            raise Rejected("threads 必须在 1..16")
                    if clone_engine == "mysqlsh":
                        ensure_local_infile(conn)
                        binary = mysqlsh_binary()
                        shell_home = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="dbm-shell-")))
                        dump_dir = str(shell_home / "dump")
                        mysqlsh_call(binary, config.url, "dump_tables", [config.target.database, selected, dump_dir,
                                     {"threads": MYSQLSH_THREADS, "bytesPerChunk": "16M", "consistent": False,
                                      "triggers": False, "showProgress": False}], timeout, shell_home)
                        if sum(path.stat().st_size for path in Path(dump_dir).iterdir()) > MAX_DUMP_BYTES:
                            raise Rejected("MySQL Shell 归档超过 128 MiB，未开始重建；请缩小 tables 范围")
                    elif selected and action != "dataset_load":
                        dump = stack.enter_context(tempfile.TemporaryFile())
                        source_dump(config, selected, action == "init_empty", timeout, dump)
                    if job is not None:
                        job.progress("来源结构/数据准备完成，开始重建镜像库")
                # Persist ownership before DDL so an interrupted initialization can be cleaned up.
                charset, collation = source_defaults(config)
                if exists:
                    if entry.get("account"):
                        close_account_sessions(conn, entry["account"])
                    conn.exec_driver_sql(f"DROP DATABASE `{database}`")
                if owned:
                    cleanup_account(conn, entry)
                state[source_id] = {"source": source}
                save_state(state_path, state)
                conn.exec_driver_sql(f"CREATE DATABASE `{database}` CHARACTER SET {charset} COLLATE {collation}")
                if action == "dataset_load":
                    if job is not None:
                        job.progress("载入测试数据集")
                    if dataset_engine == "mysqlsh":
                        with scoped_account(conn, database, state, source_id, state_path) as (user, password):
                            mysqlsh_call(binary, url.set(username=user, password=password), "load_dump", [
                                str(dump_dir),
                                {"schema": database, "threads": threads, "showProgress": False},
                            ], timeout, shell_home)
                    else:
                        with load_file.open("rb") as sql_file:
                            execute_scoped(
                                conn, url, database, sql_file, timeout, state, source_id, state_path,
                            )
                    supplement_output = None
                    supplement_sql = arguments.get("supplement_sql", "")
                    if supplement_sql:
                        if len(supplement_sql) > 1_000_000:
                            raise Rejected("supplement_sql 不超过 1000000 字符")
                        supplement_output = execute_scoped(
                            conn, url, database, supplement_sql, timeout, state, source_id, state_path,
                        )
                    verification_output = None
                    verify_sql = arguments.get("verify_sql", "")
                    if verify_sql:
                        if len(verify_sql) > 1_000_000:
                            raise Rejected("verify_sql 不超过 1000000 字符")
                        verification_output = execute_scoped(
                            conn, url, database, verify_sql, timeout, state, source_id, state_path,
                        )
                    result.update(cloneEngine=dataset_engine, datasetLoad=True)
                    if supplement_output is not None:
                        result["supplement"] = supplement_output
                    if verification_output is not None:
                        result["verification"] = verification_output
                elif clone_engine == "mysqlsh":
                    if job is not None:
                        job.progress("载入 MySQL Shell 克隆归档")
                    with scoped_account(conn, database, state, source_id, state_path) as (user, password):
                        mysqlsh_call(binary, url.set(username=user, password=password), "load_dump", [dump_dir,
                                     {"schema": database, "threads": MYSQLSH_THREADS, "showProgress": False}],
                                     timeout, shell_home)
                    result["cloneEngine"] = clone_engine
                elif dump is not None:
                    if job is not None:
                        job.progress("载入 mysqldump 归档")
                    execute_scoped(conn, url, database, dump, timeout, state, source_id, state_path)
                if job is not None:
                    job.progress("镜像库准备完成")
                return {**result, "exists": True, "owned": True, "tables": selected,
                        "message": "镜像库已准备；表结构/数据导出不包含视图、触发器、例程、事件；测试完成必须 drop"}
    except Timeout:
        raise Rejected("另一个 MCP 镜像操作正在执行，请等待其结果后再操作") from None


def run_script_test(config, arguments, job=None):
    """Prepare a managed mirror, run a client script twice, verify, and clean up."""
    sql = arguments["sql"]
    verify_sql = arguments.get("verify_sql", "").strip()
    prepare = arguments["prepare"]
    tables = arguments.get("tables", [])
    timeout = arguments.get("timeout_seconds", 60)
    if not sql.strip() or len(sql) > 1_000_000:
        raise Rejected("sql 必须非空且不超过 1000000 字符")
    if len(verify_sql) > 1_000_000:
        raise Rejected("verify_sql 不超过 1000000 字符")
    if prepare == "empty" and tables:
        raise Rejected("prepare=empty 时不能指定 tables")
    if prepare in {"init_empty", "clone"} and not tables:
        raise Rejected("init_empty/clone 必须用 tables 显式指定测试所需基础表")

    status = run_mirror(config, {"action": "status"})
    if not status.get("configured"):
        raise Rejected("尚未配置镜像管理凭据")
    if status["exists"]:
        raise Rejected("脚本测试要求没有现有镜像；请先完成或 drop 当前镜像")

    prepared = False
    runs = []
    verifications = []
    failure = None
    cleanup = None
    prepare_args = {"action": prepare, "timeout_seconds": timeout}
    if tables:
        prepare_args["tables"] = tables
    try:
        try:
            if job is not None:
                job.progress(f"准备镜像 prepare={prepare} tables={len(tables)}")
            prepared_result = run_mirror(config, prepare_args)
            prepared = bool(prepared_result.get("exists"))
            for attempt in range(1, 3):
                if job is not None:
                    job.progress(f"执行主脚本 attempt={attempt}")
                stage = "execute"
                try:
                    result = run_mirror(config, {
                        "action": "execute", "sql": sql, "timeout_seconds": timeout,
                    })
                    runs.append({"attempt": attempt, "status": "passed", **result})
                    if verify_sql:
                        if job is not None:
                            job.progress(f"执行验证 SQL attempt={attempt}")
                        stage = "verify"
                        verification = run_mirror(config, {
                            "action": "execute", "sql": verify_sql, "timeout_seconds": timeout,
                        })
                        verifications.append({"attempt": attempt, "status": "passed", **verification})
                except Exception as error:
                    failure = {
                        "stage": stage,
                        "attempt": attempt,
                        "error": str(error) if isinstance(error, Rejected) else type(error).__name__,
                    }
                    break
        except Exception as error:
            failure = {
                "stage": "prepare",
                "error": str(error) if isinstance(error, Rejected) else type(error).__name__,
            }
    finally:
        try:
            if job is not None:
                job.progress("清理脚本测试镜像")
            cleanup = run_mirror(config, {"action": "drop", "timeout_seconds": timeout})
        except Exception as cleanup_error:
            message = str(cleanup_error) if isinstance(cleanup_error, Rejected) else type(cleanup_error).__name__
            raise Rejected(f"脚本测试清理失败，不能宣称完成：{message}") from None

    output_matches = len(runs) == 2 and runs[0]["output"] == runs[1]["output"]
    verification_matches = (
        len(verifications) == 2
        and verifications[0]["output"] == verifications[1]["output"]
    ) if verify_sql else None
    passed = failure is None and output_matches and verification_matches is not False
    return {
        "action": "script_test",
        "sourceId": status.get("sourceId"),
        "source": status.get("source"),
        "database": status.get("database"),
        "prepare": prepare,
        "tables": tables,
        "prepared": prepared,
        "status": "passed" if passed else "failed",
        "runs": runs,
        "verifications": verifications,
        "outputMatches": output_matches,
        "verificationMatches": verification_matches,
        "failure": failure,
        "cleanup": {"exists": cleanup.get("exists") if cleanup else None},
        "message": (
            "脚本首次与重复执行均成功且结果一致；镜像已删除并核实不存在"
            if passed else "脚本测试未通过；镜像已删除，结果不能视为幂等或执行成功"
        ),
    }


def save_state(path, state):
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
