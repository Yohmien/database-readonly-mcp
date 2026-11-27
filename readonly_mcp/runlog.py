"""Background task logs for database MCP calls that exceed the fast-return window."""
from __future__ import annotations

import json
import os
import re
import secrets
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from .config import Rejected

ROOT = Path(__file__).resolve().parents[1]
TASK_ID = re.compile(r"[0-9A-Za-z_-]{8,64}")
DEFAULT_MAX_LOG_BYTES = 32 * 1024 * 1024
MEMORY_LIMIT_BYTES = 1024 * 1024
STALE_SECONDS = 120
DEFAULT_READ_BYTES = 16 * 1024
MAX_READ_BYTES = 64 * 1024
HEARTBEAT_SECONDS = 5
FINAL_STATUSES = {"passed", "failed", "timed_out", "cleanup_failed"}


def now_iso():
    return datetime.now().astimezone().isoformat(timespec="seconds")


def duration_text(seconds):
    seconds = int(max(0, round(seconds)))
    if seconds < 60:
        return f"{seconds} 秒"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes} 分 {seconds} 秒" if seconds else f"{minutes} 分"
    hours, minutes = divmod(minutes, 60)
    return f"{hours} 小时 {minutes} 分" if minutes else f"{hours} 小时"


def bytes_text(value):
    value = float(value)
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f} GB"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.0f} MB"
    if value >= 1_000:
        return f"{value / 1_000:.0f} KB"
    return f"{value:.0f} B"


def estimate_remaining(started_epoch, progress, now=None):
    """Estimate the remaining time of a running task from progress or a reference rate."""
    if not isinstance(progress, dict):
        return None
    started = float(started_epoch or 0)
    if started <= 0:
        return None
    now = time.time() if now is None else now
    elapsed = max(0.0, now - started)
    completed = progress.get("completed")
    total = progress.get("total")
    done_bytes = progress.get("bytes")
    total_bytes = progress.get("totalBytes")
    rate = progress.get("referenceBytesPerSecond")
    label = progress.get("referenceLabel")
    fraction = None
    if total_bytes and float(total_bytes) > 0:
        fraction = float(done_bytes or 0) / float(total_bytes)
        source = "progress"
        basis = f"已处理 {float(done_bytes or 0) / 1073741824:.2f}/{float(total_bytes) / 1073741824:.2f} GiB"
    elif total and completed is not None and float(total) > 0:
        fraction = float(completed) / float(total)
        source = "progress"
        basis = f"{int(completed)}/{int(total)} 个任务"
    else:
        fraction = None
    if rate and total_bytes and float(rate) > 0:
        done = float(done_bytes or 0)
        remaining = max(0.0, (float(total_bytes) - done) / float(rate))
        total_seconds = elapsed + remaining
        source = "reference"
        basis = label or "参考吞吐"
        if done >= 1_000_000:
            basis += f"；已完成 {bytes_text(done)}/{bytes_text(total_bytes)}"
    elif fraction is not None and fraction > 0:
        fraction = min(fraction, 1.0)
        total_seconds = elapsed / fraction
        remaining = max(0.0, total_seconds - elapsed)
    else:
        return None
    return {
        "remainingSeconds": int(round(remaining)),
        "totalSeconds": int(round(total_seconds)),
        "finishAt": datetime.fromtimestamp(now + remaining).astimezone().isoformat(timespec="seconds"),
        "source": source,
        "basis": basis,
        "text": duration_text(remaining),
    }


def log_root():
    configured = os.environ.get("DB_MCP_RUN_LOG_DIR")
    return Path(configured) if configured else ROOT / "run-logs"


def _positive_env(name, default):
    try:
        value = int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


class TaskLog:
    """Collects progress in memory and spills to disk when a call becomes long-running."""

    def __init__(self, tool, action, summary):
        self.task_id = time.strftime("%Y%m%d-%H%M%S") + "-" + secrets.token_hex(4)
        self.tool = tool
        self.action = action
        self.summary = summary or {}
        self.started_at = now_iso()
        self.started_epoch = time.time()
        self.started_monotonic = time.monotonic()
        self.heartbeat_at = self.started_at
        self.heartbeat_epoch = time.time()
        self.server_pid = os.getpid()
        self.status = "running"
        self.message = "任务仍在运行"
        self.last_progress = "starting"
        self.result = None
        self.error = None
        self.finished_at = None
        self.duration_ms = None
        self.max_bytes = _positive_env("DB_MCP_RUN_LOG_MAX_BYTES", DEFAULT_MAX_LOG_BYTES)
        self.log_path = log_root() / f"{self.task_id}.log"
        self.meta_path = log_root() / f"{self.task_id}.json"
        self._lock = threading.Lock()
        self._lines = []
        self._memory_bytes = 0
        self._spilled = False
        self._file = None
        self._written = 0
        self._truncated = False
        self._finished = False
        self._stream_pending = {"stdout": "", "stderr": ""}
        self._secret = ""
        self.progress_completed = None
        self.progress_total = None
        self.progress_bytes = None
        self.progress_total_bytes = None
        self.reference_bytes_per_second = None
        self.reference_label = None
        self.log("event=start status=running")
        self.log("params=" + json.dumps(self.summary, ensure_ascii=False)[:4000])

    @property
    def spilled(self):
        with self._lock:
            return self._spilled

    def set_secret(self, secret):
        with self._lock:
            self._secret = secret or ""

    def _redact(self, text):
        if not self._secret:
            return text
        return text.replace(self._secret, "***")

    def _line(self, message):
        return f"[{now_iso()}] {self._redact(str(message))}\n"

    def _append_locked(self, line):
        if not self._spilled:
            encoded = len(line.encode("utf-8"))
            if self._memory_bytes + encoded <= MEMORY_LIMIT_BYTES:
                self._lines.append(line)
                self._memory_bytes += encoded
                return
            self._spill_locked()
        self._write_locked(line)

    def _write_locked(self, line):
        if self._file is None:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            self._file = self.log_path.open("a", encoding="utf-8", newline="\n", buffering=1)
        encoded = len(line.encode("utf-8"))
        if self._written + encoded > self.max_bytes:
            if not self._truncated:
                marker = self._line(f"log-truncated max_bytes={self.max_bytes}")
                self._file.write(marker)
                self._file.flush()
                self._written += len(marker.encode("utf-8"))
                self._truncated = True
            return
        self._file.write(line)
        self._file.flush()
        self._written += encoded

    def _spill_locked(self):
        if self._spilled:
            return
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.log_path.open("a", encoding="utf-8", newline="\n", buffering=1)
        self._spilled = True
        pending = self._lines
        self._lines = []
        self._memory_bytes = 0
        for line in pending:
            self._write_locked(line)
        self._write_meta_locked()

    def spill(self):
        with self._lock:
            self._spill_locked()

    def _meta_locked(self):
        return {
            "taskId": self.task_id,
            "tool": self.tool,
            "action": self.action,
            "status": self.status,
            "message": self.message,
            "lastProgress": self.last_progress,
            "serverPid": self.server_pid,
            "startedAt": self.started_at,
            "startedEpoch": self.started_epoch,
            "finishedAt": self.finished_at,
            "heartbeatAt": self.heartbeat_at,
            "heartbeatEpoch": self.heartbeat_epoch,
            "durationMs": self.duration_ms,
            "logPath": str(self.log_path),
            "logBytes": self._written,
            "logTruncated": self._truncated,
            "summary": self.summary,
            "error": self.error,
            "result": self.result,
            "progress": self._progress_locked(),
        }

    def _progress_locked(self):
        data = {}
        if self.progress_completed is not None:
            data["completed"] = self.progress_completed
        if self.progress_total is not None:
            data["total"] = self.progress_total
        if self.progress_bytes is not None:
            data["bytes"] = self.progress_bytes
        if self.progress_total_bytes is not None:
            data["totalBytes"] = self.progress_total_bytes
        if self.reference_bytes_per_second is not None:
            data["referenceBytesPerSecond"] = self.reference_bytes_per_second
        if self.reference_label:
            data["referenceLabel"] = self.reference_label
        return data

    def set_reference(self, bytes_total, bytes_per_second, label=None):
        try:
            total_bytes = int(bytes_total)
            rate = float(bytes_per_second)
        except (TypeError, ValueError):
            return
        if total_bytes <= 0 or rate <= 0:
            return
        with self._lock:
            self.progress_total_bytes = total_bytes
            self.reference_bytes_per_second = rate
            self.reference_label = str(label) if label else None
            self._write_meta_locked()

    def note_progress(self, completed=None, total=None, bytes_done=None, bytes_total=None):
        with self._lock:
            if self._finished:
                return
            if completed is not None:
                self.progress_completed = int(completed)
            if total is not None:
                self.progress_total = int(total)
            if bytes_done is not None:
                self.progress_bytes = int(bytes_done)
            if bytes_total is not None:
                self.progress_total_bytes = int(bytes_total)
            self.heartbeat_at = now_iso()
            self.heartbeat_epoch = time.time()
            self._write_meta_locked()

    def estimate(self):
        with self._lock:
            return estimate_remaining(self.started_epoch, self._progress_locked())

    def _write_meta_locked(self):
        if not self._spilled:
            return
        try:
            temporary = self.meta_path.with_name(self.meta_path.name + ".tmp")
            temporary.write_text(
                json.dumps(self._meta_locked(), ensure_ascii=False, default=str),
                encoding="utf-8",
                newline="\n",
            )
            os.replace(temporary, self.meta_path)
        except (OSError, TypeError, ValueError) as error:
            print(f"database MCP task log metadata failed: {type(error).__name__}", file=sys.stderr)

    def log(self, message):
        line = self._line(message)
        with self._lock:
            if self._finished:
                return
            self._append_locked(line)

    def progress(self, message):
        message = str(message)
        with self._lock:
            if self._finished:
                return
            self.last_progress = message
            self.heartbeat_at = now_iso()
            self.heartbeat_epoch = time.time()
        self.log("progress " + message)
        with self._lock:
            self._write_meta_locked()

    def stream(self, name, data):
        if isinstance(data, bytes):
            text = data.decode("utf-8", errors="replace")
        else:
            text = str(data)
        with self._lock:
            if self._finished:
                return
            pending = self._stream_pending.get(name, "") + text
            complete = []
            tail = ""
            for item in pending.splitlines(keepends=True):
                if item.endswith("\n") or item.endswith("\r"):
                    complete.append(item.rstrip("\r\n"))
                else:
                    tail = item
            self._stream_pending[name] = tail
            for item in complete:
                self._append_locked(self._line(f"{name}: {item}"))

    def _flush_streams_locked(self):
        for name, pending in self._stream_pending.items():
            if pending:
                self._append_locked(self._line(f"{name}: {pending}"))
        self._stream_pending = {"stdout": "", "stderr": ""}

    def finish(self, result=None, error=None):
        with self._lock:
            if self._finished:
                return
            self._finished = True
            self._flush_streams_locked()
            if error is not None:
                self.status = "failed"
                self.message = str(error.get("error") or "执行失败")
            elif isinstance(result, dict) and result.get("status") in FINAL_STATUSES:
                self.status = result["status"]
                self.message = str(result.get("message") or self.status)
            else:
                self.status = "completed"
                self.message = "执行完成"
            self.result = result
            self.error = error
            self.finished_at = now_iso()
            self.duration_ms = int((time.monotonic() - self.started_monotonic) * 1000)
            self.heartbeat_at = self.finished_at
            self.heartbeat_epoch = time.time()
            if self._spilled:
                summary = {"status": self.status, "durationMs": self.duration_ms}
                if error is not None:
                    summary["error"] = error
                elif isinstance(result, dict):
                    for key in ("exitCode", "timedOut", "database", "action",
                                "verificationStatus", "extensionId"):
                        if key in result:
                            summary[key] = result[key]
                self._append_locked(self._line(
                    "event=finish " + json.dumps(summary, ensure_ascii=False, default=str)
                ))
                self._write_meta_locked()
            if self._file is not None:
                self._file.close()
                self._file = None

    def running_payload(self):
        self.spill()
        estimate = self.estimate()
        with self._lock:
            payload = {
                "taskId": self.task_id,
                "status": "running",
                "tool": self.tool,
                "action": self.action,
                "logPath": str(self.log_path),
                "readTool": "db_task_result",
                "readHint": f'db_task_result {{"task_id":"{self.task_id}"}}',
                "message": "执行超过 10 秒，仍在后台运行；使用 db_task_result 读取进度和最终结果",
                "startedAt": self.started_at,
                "heartbeatAt": self.heartbeat_at,
                "durationMs": int((time.monotonic() - self.started_monotonic) * 1000),
            }
        if estimate:
            payload["estimate"] = estimate
            payload["message"] = (
                "执行超过 10 秒，仍在后台运行；预计还需约 "
                f"{estimate['text']}；使用 db_task_result 读取进度和最终结果"
            )
        return payload

    def with_reference(self, result):
        with self._lock:
            if not self._spilled:
                return result
            if not isinstance(result, dict):
                result = {"result": result}
            return {
                **result,
                "taskId": self.task_id,
                "logPath": str(self.log_path),
                "readTool": "db_task_result",
            }


def result_summary(result):
    if not isinstance(result, dict):
        return None
    keys = ("status", "message", "action", "database", "exitCode", "timedOut",
            "durationMs", "durationSeconds", "verificationStatus", "extensionId",
            "extensionStatus", "outputMatches", "verificationMatches", "prepared",
            "truncated", "elapsedMs", "count", "rowCount", "bytes")
    summary = {key: result[key] for key in keys
               if key in result and isinstance(result[key], (str, int, float, bool))}
    if isinstance(result.get("columns"), list):
        summary["columns"] = [str(item) for item in result["columns"][:50]]
        summary["columnCount"] = len(result["columns"])
    if isinstance(result.get("rows"), list):
        summary["rowCount"] = len(result["rows"])
    elif isinstance(result.get("values"), list):
        summary["rowCount"] = len(result["values"])
    if isinstance(result.get("tables"), list):
        summary["tableCount"] = len(result["tables"])
    if isinstance(result.get("runs"), list):
        summary["runCount"] = len(result["runs"])
    if isinstance(result.get("verifications"), list):
        summary["verificationCount"] = len(result["verifications"])
    archive = result.get("archive")
    if isinstance(archive, dict):
        summary["archiveEngine"] = archive.get("engine")
        summary["archiveBytes"] = archive.get("bytes")
        summary["archiveFallbackReason"] = archive.get("fallbackReason")
    for key in ("failure", "verificationError", "error"):
        if key in result:
            summary[key] = result[key]
    if result.get("status") in {"failed", "timed_out", "cleanup_failed"}:
        for key in ("stdout", "stderr"):
            if isinstance(result.get(key), str):
                summary[key] = result[key]
    cleanup = result.get("cleanup")
    if cleanup is not None:
        try:
            compact = json.dumps(cleanup, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            compact = None
        if compact is not None and len(compact) <= 1500:
            summary["cleanup"] = cleanup
    return summary


def read_task(task_id, offset=None, max_bytes=DEFAULT_READ_BYTES, include_result=False,
              log_mode="summary", line_start=None, line_end=None):
    if not isinstance(task_id, str) or not TASK_ID.fullmatch(task_id):
        raise Rejected("task_id 格式无效；应从超过 10 秒的数据库 MCP 调用结果中获取")
    root = log_root()
    meta_path = root / f"{task_id}.json"
    log_path = root / f"{task_id}.log"
    meta = {}
    if meta_path.is_file():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            raise Rejected("任务元数据损坏，无法读取执行状态") from None
    elif not log_path.is_file():
        raise Rejected("任务不存在或尚未生成日志；请使用调用返回的 task_id")

    status = meta.get("status", "unknown")
    message = meta.get("message")
    if status == "running":
        heartbeat = float(meta.get("heartbeatEpoch") or 0)
        if meta.get("serverPid") != os.getpid():
            status = "interrupted"
            message = "MCP 服务已重启，原后台任务不再由当前进程执行；结果可能不完整"
        elif time.time() - heartbeat > STALE_SECONDS:
            status = "interrupted"
            message = "后台任务心跳已停止；结果可能不完整，请结合日志与产物核对"

    size = log_path.stat().st_size if log_path.is_file() else 0
    selected_start = None
    selected_end = None
    selected_count = 0
    next_offset = None
    eof = True
    text = ""
    if line_start is not None:
        line_start = int(line_start)
        line_end = int(line_end) if line_end is not None else None
        if line_start < 1 or (line_end is not None and line_end < line_start):
            raise Rejected("line_start/line_end 必须为正整数，且 line_end 不小于 line_start")
        selected_start = line_start
        selected_end = line_end if line_end is not None else line_start
        selected = []
        if log_path.is_file():
            with log_path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
                for number, line in enumerate(handle, 1):
                    if number > selected_end:
                        break
                    if number >= selected_start:
                        selected.append(line.rstrip("\r\n"))
        text = "\n".join(selected)
        selected_count = len(selected)
        log_mode = "lines"
    elif log_mode == "full":
        if log_path.is_file():
            text = log_path.read_text(encoding="utf-8", errors="replace")
        next_offset = size
    elif log_mode == "tail":
        max_bytes = max(1, min(int(max_bytes), MAX_READ_BYTES))
        offset = max(0, size - max_bytes) if offset is None else max(0, min(int(offset), size))
        data = b""
        if log_path.is_file():
            with log_path.open("rb") as handle:
                handle.seek(offset)
                data = handle.read(max_bytes)
        text = data.decode("utf-8", errors="replace")
        next_offset = offset + len(data)
        eof = next_offset >= size
    elif log_mode != "summary":
        raise Rejected("log_mode 仅支持 summary、tail、full")
    estimate = estimate_remaining(meta.get("startedEpoch"), meta.get("progress")) if status == "running" else None
    error = meta.get("error")
    summary = result_summary(meta.get("result"))
    if error is not None:
        if summary is None:
            summary = {}
        summary["error"] = error
        if isinstance(error, dict):
            for key in ("kind", "type", "code", "errno"):
                if key in error:
                    summary["error" + key.title()] = error[key]
    return {
        "taskId": task_id,
        "status": status,
        "message": message,
        "lastProgress": meta.get("lastProgress"),
        "tool": meta.get("tool"),
        "action": meta.get("action"),
        "startedAt": meta.get("startedAt"),
        "finishedAt": meta.get("finishedAt"),
        "heartbeatAt": meta.get("heartbeatAt"),
        "durationMs": meta.get("durationMs"),
        "exitCode": (meta.get("result") or {}).get("exitCode")
        if isinstance(meta.get("result"), dict) else None,
        "logPath": str(log_path),
        "logSize": size,
        "logTruncated": meta.get("logTruncated", False),
        "logMode": log_mode,
        "offset": offset,
        "nextOffset": next_offset,
        "eof": eof,
        "lineStart": selected_start,
        "lineEnd": selected_end,
        "lineCount": selected_count,
        "text": text,
        "error": error,
        "estimate": estimate,
        "resultSummary": summary,
        "fullResultAvailable": isinstance(meta.get("result"), dict),
        "result": meta.get("result")
        if include_result and status != "running" else None,
        "readTool": "db_task_result",
        "next": f'db_task_result {{"task_id":"{task_id}","log_mode":"tail","offset":{next_offset}}}'
        if log_mode == "tail" and not eof else None,
    }
