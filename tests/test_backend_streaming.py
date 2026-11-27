"""EXPLAIN 的服务端游标行为：PostgreSQL 必须关闭，MySQL/MariaDB 保持原样。"""
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

from readonly_mcp.backend import Backend


class _Result:
    def keys(self):
        return ["c1"]

    def fetchmany(self, count):
        return [("v",)]

    def close(self):
        pass


class _Conn:
    def __init__(self):
        self.options = None

    def execution_options(self, **kwargs):
        self.options = kwargs
        return self

    def execute(self, statement, parameters):
        return _Result()


class _Config:
    max_rows = 100
    allowed_databases = None
    name = "test-conn"

    def __init__(self, dialect):
        self.target = SimpleNamespace(dialect=dialect, database="testdb")


class _Backend(Backend):
    """只替换会话来源，保留真实的 query() 执行路径。"""

    def __init__(self, dialect, conn):
        self.config = _Config(dialect)
        self.dialect = dialect
        self._conn = conn

    def session(self):
        @contextmanager
        def _session():
            yield self._conn, []

        return _session()


class StreamingTests(unittest.TestCase):
    def _stream_results(self, dialect, sql):
        conn = _Conn()
        with patch("readonly_mcp.backend.validate_sql"):
            _Backend(dialect, conn).query(sql)
        return conn.options.get("stream_results")

    def test_postgres_explain_avoids_server_side_cursor(self):
        # stream_results 会生成 DECLARE ... CURSOR FOR EXPLAIN，PostgreSQL 报 42601
        self.assertFalse(self._stream_results("postgresql", "EXPLAIN select 1"))

    def test_postgres_select_still_streams(self):
        self.assertTrue(self._stream_results("postgresql", "select 1"))

    def test_mysql_and_mariadb_keep_streaming(self):
        for dialect in ("mysql", "mariadb"):
            with self.subTest(dialect=dialect):
                self.assertTrue(self._stream_results(dialect, "EXPLAIN select 1"))
                self.assertTrue(self._stream_results(dialect, "select 1"))

    def test_sqlite_never_streams(self):
        self.assertFalse(self._stream_results("sqlite", "select 1"))
        self.assertFalse(self._stream_results("sqlite", "EXPLAIN QUERY PLAN select 1"))


if __name__ == "__main__":
    unittest.main()
