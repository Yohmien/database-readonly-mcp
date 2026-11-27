import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from sqlalchemy.engine import URL

from readonly_mcp.config import Connection, Rejected
from readonly_mcp.mirror import (
    StreamingRedactor,
    identity,
    redact_secret,
    run_child_process,
    test_environment,
    validate_test_command,
)
from readonly_mcp.server import run_tool


class RunTestTests(unittest.TestCase):
    def setUp(self):
        self.entry = Connection(
            "source",
            URL.create(
                "mysql+pymysql",
                username="readonly",
                password="source-secret",
                host="localhost",
                port=3306,
                database="warehouse",
            ),
        )

    def test_environment_is_scoped_and_has_no_admin_credentials(self):
        env = test_environment(self.entry, "dbm0123456789", "dbmuser", "test-secret")
        self.assertEqual(env["DB_MCP_TEST_DATABASE"], "dbm0123456789")
        self.assertEqual(env["DB_MCP_TEST_USER"], "dbmuser")
        self.assertEqual(env["DB_MCP_TEST_PASSWORD"], "test-secret")
        self.assertEqual(env["SPRING_DATASOURCE_PASSWORD"], "test-secret")
        self.assertNotIn("test-secret", env["DB_MCP_TEST_URL"])
        self.assertNotIn("test-secret", env["DB_MCP_TEST_JDBC_URL"])
        self.assertNotIn("DB_MCP_MIRROR_ADMIN_PASSWORD", env)
        self.assertNotIn("MYSQL_PASS", env)

    def test_environment_rejects_tls_until_supported(self):
        secure = Connection(
            "secure",
            URL.create(
                "mysql+pymysql",
                username="readonly",
                host="localhost",
                database="warehouse",
                query={"ssl_ca": "ca.pem"},
            ),
        )
        with self.assertRaises(Rejected):
            test_environment(secure, "dbm0123456789", "dbmuser", "test-secret")

    def test_redaction_covers_plain_and_url_encoded_values(self):
        secret = "a-b_c"
        self.assertEqual(redact_secret(f"x={secret}; y=a-b_c", secret), "x=***; y=***")

    def test_streaming_redaction_handles_chunk_and_limit_boundaries(self):
        secret = "s" * 44
        limit = 64 * 1024
        redactor = StreamingRedactor(secret, limit)
        redactor.feed(b"A" * (limit - 41 + 20) + secret[:10].encode())
        redactor.feed(secret[10:].encode() + b"Z" * 100)
        redactor.finish()
        text = redactor.text()
        self.assertTrue(redactor.truncated)
        self.assertIn("***", text)
        self.assertNotIn(secret[:41], text)
        self.assertNotIn(secret, text)
        self.assertLessEqual(len(text), limit)

    def test_streaming_redaction_covers_url_encoded_form(self):
        redactor = StreamingRedactor("a b", 100)
        redactor.feed(b"prefix:a%20b:suffix")
        redactor.finish()
        self.assertEqual(redactor.text(), "prefix:***:suffix")

    def test_command_validation_rejects_shell_and_bad_cwd(self):
        cwd = Path.cwd()
        with self.assertRaises(Rejected):
            validate_test_command([], cwd)
        with self.assertRaises(Rejected):
            validate_test_command(["cmd.exe", "/c", "echo hi"], cwd)
        with self.assertRaises(Rejected):
            validate_test_command([sys.executable, "script.py"], Path("relative"))
        with self.assertRaises(Rejected):
            validate_test_command([sys.executable, "nul\x00break"], cwd)
        with self.assertRaises(Rejected):
            validate_test_command([sys.executable, "secret"], cwd, "secret")

    def test_child_success_redacts_password(self):
        code = (
            "import os; print('PASSWORD=' + os.environ['DB_MCP_TEST_PASSWORD']); "
            "print('MCP_RUN_TEST_OK')"
        )
        env = {
            "PATH": os.environ.get("PATH", ""),
            "SYSTEMROOT": os.environ.get("SystemRoot", ""),
            "DB_MCP_TEST_PASSWORD": "child-secret",
        }
        result = run_child_process(
            [sys.executable, "-c", code], Path.cwd(), env, "child-secret", 10
        )
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["exitCode"], 0)
        self.assertIn("PASSWORD=***", result["stdout"])
        self.assertIn("MCP_RUN_TEST_OK", result["stdout"])
        self.assertNotIn("child-secret", result["stdout"] + result["stderr"])

    def test_child_nonzero_is_reported(self):
        result = run_child_process(
            [sys.executable, "-c", "raise SystemExit(7)"],
            Path.cwd(),
            {"PATH": os.environ.get("PATH", ""), "SYSTEMROOT": os.environ.get("SystemRoot", "")},
            "",
            10,
        )
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["exitCode"], 7)
        self.assertFalse(result["timedOut"])

    def test_child_output_is_bounded(self):
        result = run_child_process(
            [sys.executable, "-c", "print('x' * 70000)"],
            Path.cwd(),
            {"PATH": os.environ.get("PATH", ""), "SYSTEMROOT": os.environ.get("SystemRoot", "")},
            "",
            10,
        )
        self.assertTrue(result["truncated"])
        self.assertLessEqual(len(result["stdout"]), 64 * 1024)

    def test_child_redacts_secret_at_both_stream_boundaries(self):
        secret = "s" * 44
        code = (
            "import sys\n"
            "pad = 'A' * (64 * 1024 - 41)\n"
            "sys.stdout.write(pad + " + repr(secret) + " + 'Z' * 100)\n"
            "sys.stderr.write(pad + " + repr(secret) + " + 'Y' * 100)\n"
        )
        result = run_child_process(
            [sys.executable, "-c", code],
            Path.cwd(),
            {"PATH": os.environ.get("PATH", ""), "SYSTEMROOT": os.environ.get("SystemRoot", "")},
            secret,
            10,
        )
        self.assertEqual(result["status"], "passed")
        self.assertTrue(result["truncated"])
        for key in ("stdout", "stderr"):
            self.assertIn("***", result[key])
            self.assertNotIn(secret[:41], result[key])
            self.assertNotIn(secret, result[key])

    def test_timeout_kills_grandchild(self):
        code = (
            "import subprocess, sys, time\n"
            "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])\n"
            "print(child.pid, flush=True)\n"
            "time.sleep(30)\n"
        )
        result = run_child_process(
            [sys.executable, "-c", code],
            Path.cwd(),
            {"PATH": os.environ.get("PATH", ""), "SYSTEMROOT": os.environ.get("SystemRoot", "")},
            "",
            3,
        )
        self.assertEqual(result["status"], "timed_out")
        self.assertTrue(result["timedOut"])
        self.assertTrue(result["stdout"].strip(), result)
        pid = int(result["stdout"].splitlines()[0])
        if os.name == "nt":
            time.sleep(0.5)
            listing = subprocess.run(
                ["tasklist", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                timeout=10,
            ).stdout
            self.assertNotIn(f'"{pid}"', listing)
        else:
            with self.assertRaises(OSError):
                os.kill(pid, 0)

    def test_normal_exit_kills_leftover_grandchild(self):
        code = (
            "import subprocess, sys\n"
            "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])\n"
            "print(child.pid, flush=True)\n"
        )
        result = run_child_process(
            [sys.executable, "-c", code],
            Path.cwd(),
            {"PATH": os.environ.get("PATH", ""), "SYSTEMROOT": os.environ.get("SystemRoot", "")},
            "",
            10,
        )
        self.assertEqual(result["status"], "passed")
        self.assertTrue(result["stdout"].strip(), result)
        pid = int(result["stdout"].splitlines()[0])
        time.sleep(0.5)
        if os.name == "nt":
            listing = subprocess.run(
                ["tasklist", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                timeout=10,
            ).stdout
            self.assertNotIn(f'"{pid}"', listing)
        else:
            with self.assertRaises(OSError):
                os.kill(pid, 0)

    def test_nonzero_exit_kills_leftover_grandchild(self):
        code = (
            "import subprocess, sys\n"
            "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])\n"
            "print(child.pid, flush=True)\n"
            "raise SystemExit(7)\n"
        )
        result = run_child_process(
            [sys.executable, "-c", code],
            Path.cwd(),
            {"PATH": os.environ.get("PATH", ""), "SYSTEMROOT": os.environ.get("SystemRoot", "")},
            "",
            10,
        )
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["exitCode"], 7)
        self.assertTrue(result["stdout"].strip(), result)
        pid = int(result["stdout"].splitlines()[0])
        time.sleep(0.5)
        if os.name == "nt":
            listing = subprocess.run(
                ["tasklist", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                timeout=10,
            ).stdout
            self.assertNotIn(f'"{pid}"', listing)
        else:
            with self.assertRaises(OSError):
                os.kill(pid, 0)

    @unittest.skipUnless(os.name == "nt", "Windows Job Object 边界")
    def test_job_binding_failure_rejects_before_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "payload-ran"
            payloads = {
                "normal": f"from pathlib import Path; Path({str(marker)!r}).write_text('x')",
                "nonzero": (
                    f"from pathlib import Path; Path({str(marker)!r}).write_text('x'); "
                    "raise SystemExit(7)"
                ),
                "timeout": (
                    f"from pathlib import Path; Path({str(marker)!r}).write_text('x'); "
                    "import time; time.sleep(30)"
                ),
            }
            for name, code in payloads.items():
                with self.subTest(name=name), \
                        patch("readonly_mcp.mirror._attach_windows_job",
                              side_effect=OSError("synthetic job failure")):
                    with self.assertRaises(Rejected):
                        run_child_process(
                            [sys.executable, "-c", code],
                            Path.cwd(),
                            {"PATH": os.environ.get("PATH", ""),
                             "SYSTEMROOT": os.environ.get("SystemRoot", "")},
                            "",
                            10,
                        )
                self.assertFalse(marker.exists(), name)

    def test_run_test_requires_existing_mirror(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            with patch.dict(os.environ, {"DB_MCP_MIRROR_STATE_FILE": str(state)}), \
                    patch("readonly_mcp.mirror.admin_url", return_value=self.entry.url), \
                    patch("readonly_mcp.mirror.resolved_identity",
                          return_value=identity(self.entry, "test-instance", "warehouse")):
                @contextmanager
                def session(*args):
                    class Row:
                        def scalar(self):
                            return 0

                    class Conn:
                        def execute(self, *args, **kwargs):
                            return Row()

                        def exec_driver_sql(self, *args, **kwargs):
                            return Row()

                    yield Conn()

                with patch("readonly_mcp.mirror.admin_session", session):
                    with self.assertRaises(Rejected):
                        run_tool(
                            "db_mirror",
                            {
                                "action": "run_test",
                                "connection": "source",
                                "command": [sys.executable, "-c", "print(1)"],
                                "cwd": str(Path.cwd()),
                            },
                            [self.entry],
                        )

    def test_run_test_rejects_unowned_existing_mirror(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            source_id, database, source = identity(self.entry, "test-instance", "warehouse")

            @contextmanager
            def session(*args):
                class Row:
                    def scalar(self):
                        return 1

                class Conn:
                    def execute(self, *args, **kwargs):
                        return Row()

                    def exec_driver_sql(self, *args, **kwargs):
                        return Row()

                yield Conn()

            with patch.dict(os.environ, {"DB_MCP_MIRROR_STATE_FILE": str(state)}), \
                    patch("readonly_mcp.mirror.admin_url", return_value=self.entry.url), \
                    patch("readonly_mcp.mirror.admin_session", session), \
                    patch("readonly_mcp.mirror.resolved_identity",
                          return_value=(source_id, database, source)), \
                    patch("readonly_mcp.mirror.run_test_scoped") as runner:
                with self.assertRaises(Rejected):
                    run_tool(
                        "db_mirror",
                        {
                            "action": "run_test",
                            "connection": "source",
                            "command": [sys.executable, "-c", "print(1)"],
                            "cwd": str(Path.cwd()),
                        },
                        [self.entry],
                    )
                runner.assert_not_called()

    def test_run_test_dispatches_to_scoped_runner(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            source_id, database, source = identity(self.entry, "test-instance", "warehouse")
            state.write_text(json.dumps({source_id: {"source": source}}), encoding="utf-8")

            @contextmanager
            def session(*args):
                class Row:
                    def scalar(self):
                        return 1

                class Conn:
                    def execute(self, *args, **kwargs):
                        return Row()

                    def exec_driver_sql(self, *args, **kwargs):
                        return Row()

                yield Conn()

            expected = {"status": "passed", "exitCode": 0}
            with patch.dict(os.environ, {"DB_MCP_MIRROR_STATE_FILE": str(state)}), \
                    patch("readonly_mcp.mirror.admin_url", return_value=self.entry.url), \
                    patch("readonly_mcp.mirror.admin_session", session), \
                    patch("readonly_mcp.mirror.resolved_identity",
                          return_value=(source_id, database, source)), \
                    patch("readonly_mcp.mirror.run_test_scoped", return_value=expected) as runner:
                result = run_tool(
                    "db_mirror",
                    {
                        "action": "run_test",
                        "connection": "source",
                        "command": [sys.executable, "-c", "print(1)"],
                        "cwd": str(Path.cwd()),
                    },
                    [self.entry],
                ).structuredContent
                self.assertEqual(result["status"], "passed")
                runner.assert_called_once()


if __name__ == "__main__":
    unittest.main()
