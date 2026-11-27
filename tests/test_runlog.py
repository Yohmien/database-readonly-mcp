import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from readonly_mcp import server
from readonly_mcp.config import Rejected
from readonly_mcp.mirror import run_child_process
from readonly_mcp.runlog import TaskLog, estimate_remaining, read_task


class RunLogTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {"DB_MCP_RUN_LOG_DIR": self.temp.name})
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def test_fast_task_keeps_logs_in_memory(self):
        task = TaskLog("db_query", None, {"database": "warehouse"})
        task.progress("step")
        task.finish(result={"columns": ["n"], "rows": [{"n": 1}]})
        self.assertFalse(Path(task.log_path).exists())
        self.assertFalse(Path(task.meta_path).exists())

    def test_spilled_task_reads_progress_and_incremental_text(self):
        task = TaskLog("db_mirror", "run_test", {"database": "warehouse"})
        task.spill()
        task.progress("step-1")
        task.stream("stdout", b"hello\npartial")
        task.finish(result={"status": "passed", "exitCode": 0, "message": "ok"})

        summary = read_task(task.task_id)
        self.assertEqual(summary["logMode"], "summary")
        self.assertEqual(summary["text"], "")
        self.assertEqual(summary["resultSummary"]["exitCode"], 0)
        self.assertIsNone(summary["result"])

        complete = read_task(task.task_id, log_mode="full")
        self.assertEqual(complete["status"], "passed")
        self.assertEqual(complete["logMode"], "full")
        self.assertIn("step-1", complete["text"])
        self.assertIn("stdout: hello", complete["text"])
        self.assertIn("stdout: partial", complete["text"])
        self.assertEqual(complete["resultSummary"]["exitCode"], 0)
        self.assertIsNone(complete["result"])
        self.assertTrue(complete["eof"])

        first = read_task(task.task_id, 0, 20, log_mode="tail")
        second = read_task(task.task_id, first["nextOffset"], log_mode="tail")
        self.assertFalse(first["eof"])
        self.assertEqual(first["text"] + second["text"], complete["text"])
        tail = read_task(task.task_id, log_mode="tail", max_bytes=20)
        self.assertEqual(tail["offset"], max(0, complete["logSize"] - 20))
        self.assertEqual(tail["text"], complete["text"][-20:])

        selected = read_task(task.task_id, line_start=4, line_end=5)
        self.assertEqual(selected["logMode"], "lines")
        self.assertEqual(selected["lineCount"], 2)
        self.assertIn("stdout: hello", selected["text"])
        self.assertIn("stdout: partial", selected["text"])

    def test_run_tool_returns_task_when_call_exceeds_window(self):
        def slow_dispatch(*args, **kwargs):
            time.sleep(0.3)
            return {"value": 7}

        with patch.object(server, "FAST_RETURN_SECONDS", 0.05), \
                patch.object(server, "dispatch", side_effect=slow_dispatch):
            response = server.run_tool(
                "db_sql_check", {"sql": "SELECT 1", "dialect": "mysql"}, []
            ).structuredContent
        self.assertEqual(response["status"], "running")
        self.assertEqual(response["readTool"], "db_task_result")
        self.assertTrue(Path(response["logPath"]).is_file())
        self.assertIn(response["taskId"], response["readHint"])

        result = None
        for _ in range(100):
            result = server.run_tool(
                "db_task_result", {"task_id": response["taskId"]}, []
            ).structuredContent
            if result["status"] != "running":
                break
            time.sleep(0.02)
        self.assertEqual(result["status"], "completed")
        self.assertIsNone(result["result"])
        self.assertTrue(result["fullResultAvailable"])
        self.assertEqual(result["text"], "")
        full_log = server.run_tool(
            "db_task_result",
            {"task_id": response["taskId"], "log_mode": "full"},
            [],
        ).structuredContent
        self.assertIn("event=finish", full_log["text"])
        full = server.run_tool(
            "db_task_result",
            {"task_id": response["taskId"], "include_result": True},
            [],
        ).structuredContent
        self.assertEqual(full["result"]["value"], 7)

    def test_reference_estimate_is_reported_for_running_import(self):
        task = TaskLog("db_sql_import", "import", {"sql_path": "C:/dump.sql"})
        task.set_reference(6_615_558_946, 6_615_558_946 / 511.593, "参考任务 sample")
        payload = task.running_payload()
        self.assertEqual(payload["estimate"]["source"], "reference")
        self.assertGreater(payload["estimate"]["remainingSeconds"], 400)
        self.assertLessEqual(payload["estimate"]["remainingSeconds"], 512)
        self.assertIn("预计还需约", payload["message"])
        self.assertTrue(Path(task.meta_path).exists())
        task.finish(result={"status": "completed"})

    def test_estimate_uses_actual_progress_and_is_visible_in_task_result(self):
        now = time.time()
        estimate = estimate_remaining(now - 10, {"bytes": 500, "totalBytes": 1000}, now=now)
        self.assertEqual(estimate["source"], "progress")
        self.assertEqual(estimate["remainingSeconds"], 10)
        self.assertEqual(estimate["totalSeconds"], 20)
        reference = estimate_remaining(
            now - 10,
            {"bytes": 0, "totalBytes": 1000, "referenceBytesPerSecond": 10},
            now=now,
        )
        self.assertEqual(reference["source"], "reference")
        self.assertEqual(reference["remainingSeconds"], 100)
        self.assertEqual(reference["basis"], "参考吞吐")
        partial = estimate_remaining(
            now - 10,
            {"bytes": 200, "totalBytes": 1000, "referenceBytesPerSecond": 10},
            now=now,
        )
        self.assertEqual(partial["remainingSeconds"], 80)
        self.assertEqual(partial["totalSeconds"], 90)
        sized = estimate_remaining(
            now - 10,
            {"bytes": 2_000_000, "totalBytes": 10_000_000, "referenceBytesPerSecond": 1_000_000},
            now=now,
        )
        self.assertEqual(sized["remainingSeconds"], 8)
        self.assertIn("已完成 2 MB/10 MB", sized["basis"])
        task = TaskLog("db_sql_import", "import", {})
        task.set_reference(1000, 10, "ref")
        task.note_progress(completed=5, total=10, bytes_done=500, bytes_total=1000)
        task.spill()
        info = read_task(task.task_id)
        self.assertEqual(info["status"], "running")
        self.assertEqual(info["estimate"]["source"], "reference")
        self.assertEqual(info["estimate"]["basis"], "ref")
        task.finish(result={"status": "completed"})

    def test_fast_error_is_raised_to_caller(self):
        with patch.object(server, "dispatch", side_effect=Rejected("bad request")):
            with self.assertRaises(Rejected):
                server.run_tool("db_sql_check", {"sql": "SELECT 1", "dialect": "mysql"}, [])

    def test_failed_result_summary_keeps_complete_output(self):
        task = TaskLog("db_mirror", "run_test", {"database": "warehouse"})
        task.spill()
        task.finish(result={
            "status": "failed",
            "exitCode": 7,
            "stdout": "OUT",
            "stderr": "ERR",
            "message": "测试进程非 0 退出",
        })
        result = read_task(task.task_id)
        self.assertEqual(result["resultSummary"]["exitCode"], 7)
        self.assertEqual(result["resultSummary"]["stdout"], "OUT")
        self.assertEqual(result["resultSummary"]["stderr"], "ERR")

    def test_slow_error_returns_complete_safe_summary(self):
        def failing_dispatch(*args, **kwargs):
            time.sleep(0.3)
            raise Rejected("完整异常信息：镜像库不存在")

        with patch.object(server, "FAST_RETURN_SECONDS", 0.05), \
                patch.object(server, "dispatch", side_effect=failing_dispatch):
            response = server.run_tool(
                "db_sql_check", {"sql": "SELECT 1", "dialect": "mysql"}, []
            ).structuredContent
        result = None
        for _ in range(100):
            result = server.run_tool(
                "db_task_result", {"task_id": response["taskId"]}, []
            ).structuredContent
            if result["status"] != "running":
                break
            time.sleep(0.02)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["error"]["kind"], "rejected")
        self.assertEqual(result["error"]["error"], "完整异常信息：镜像库不存在")
        self.assertEqual(result["resultSummary"]["errorKind"], "rejected")
        self.assertEqual(
            result["resultSummary"]["error"]["error"], "完整异常信息：镜像库不存在"
        )
        full = server.run_tool(
            "db_task_result",
            {"task_id": response["taskId"], "log_mode": "full"},
            [],
        ).structuredContent
        self.assertIn("event=error", full["text"])
        self.assertIn("完整异常信息", full["text"])

    def test_missing_task_is_rejected(self):
        with self.assertRaises(Rejected):
            read_task("00000000-0000")
        with self.assertRaises(Rejected):
            read_task("../escape")

    def test_child_process_writes_redacted_stream_log(self):
        task = TaskLog("db_mirror", "run_test", {"database": "warehouse"})
        task.spill()
        secret = "child-secret"
        code = (
            "import os; print('PASSWORD=' + os.environ['DB_MCP_TEST_PASSWORD']); "
            "print('CHILD_OK')"
        )
        result = run_child_process(
            [sys.executable, "-c", code],
            Path.cwd(),
            {
                "PATH": os.environ.get("PATH", ""),
                "SYSTEMROOT": os.environ.get("SystemRoot", ""),
                "DB_MCP_TEST_PASSWORD": secret,
            },
            secret,
            10,
            job=task,
        )
        task.finish(result=result)
        data = read_task(task.task_id, log_mode="full")
        self.assertEqual(data["status"], "passed")
        self.assertIn("stdout: PASSWORD=***", data["text"])
        self.assertIn("stdout: CHILD_OK", data["text"])
        self.assertNotIn(secret, data["text"])


if __name__ == "__main__":
    unittest.main()
