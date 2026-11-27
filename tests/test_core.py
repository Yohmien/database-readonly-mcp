import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from sqlalchemy.engine import URL

from readonly_mcp.backend import Backend, verify_mysql_grants
from readonly_mcp.config import Connection, Rejected, load_connections, project_targets, resolve, target_from_url, validate_url_options
from readonly_mcp.guard import validate_sql
from readonly_mcp.server import run_tool, safe_error
from sqlalchemy.engine import make_url


class GuardTests(unittest.TestCase):
    def test_valid_queries(self):
        cases = [("SELECT COUNT(*), COALESCE(SUM(a),0) FROM `db-a`.`t`", "mysql"),
                 ("WITH t AS (SELECT 1 AS n) SELECT n FROM t UNION ALL SELECT 2", "postgresql"),
                 ("SELECT :name AS name", "sqlite"), ("SELECT TOP 10 * FROM dbo.t", "mssql"),
                 ("SELECT NVL(NULL,1) FROM DUAL", "oracle"),
                 ("SELECT 'DELETE; /*!' AS x", "sqlite"),
                 ("SELECT ROW_NUMBER() OVER (ORDER BY a) FROM t", "postgresql")]
        for sql, dialect in cases:
            with self.subTest(sql=sql):
                if "/*!" in sql:
                    with self.assertRaises(Rejected):
                        validate_sql(sql, dialect)
                else:
                    self.assertEqual(validate_sql(sql, dialect), "select")

    def test_reject_writes_and_side_effects(self):
        cases = ["DELETE FROM t", "SELECT 1; SELECT 2", "SELECT 1;;",
                 "WITH t AS (DELETE FROM x RETURNING *) SELECT * FROM t",
                 "SELECT * FROM t FOR UPDATE", "SELECT * FROM (SELECT * FROM t FOR UPDATE) s",
                 "SELECT * FROM t LOCK IN SHARE MODE", "SELECT * FROM t FOR SHARE",
                 "SELECT * INTO new_t FROM t", "SELECT 1 INTO OUTFILE '/tmp/x'",
                 "SELECT 1 /*! INTO OUTFILE '/tmp/x' */", "SELECT sleep(1)",
                 "SELECT GET_LOCK('x',1)", "SELECT load_file('/tmp/x')",
                 "SELECT unknown_function(1)", "SELECT evil.ABS(1)",
                 "SELECT @x:=1", "CALL x()", "SET a=1", "USE x", "CREATE TABLE t(a INT)",
                 "EXPLAIN ANALYZE SELECT 1", "EXPLAIN DELETE FROM t", "PRAGMA user_version=9",
                 "SELECT * FROM OPENROWSET('a','b','c')", "SELECT * FROM server.db.dbo.t"]
        for sql in cases:
            with self.subTest(sql=sql), self.assertRaises(Rejected):
                validate_sql(sql, "mysql")
        for sql in ("SELECT nextval('x')", "SELECT pg_notify('x','x')", "SELECT pg_sleep(1)"):
            with self.subTest(sql=sql), self.assertRaises(Rejected):
                validate_sql(sql, "postgresql")

    def test_metadata_and_explain(self):
        for sql in ("SHOW GRANTS", "SHOW DATABASES", "DESCRIBE `db-a`.`t`", "SHOW COLUMNS FROM t FROM `db-a`", "SHOW CREATE TABLE `db-a`.`t`", "EXPLAIN FORMAT=JSON SELECT 1"):
            self.assertIn(validate_sql(sql, "mysql"), {"grants", "metadata", "explain"})
        for sql in ("SHOW GRANTS FOR other", "SHOW PROCESSLIST", "DESCRIBE t; DROP TABLE t", "EXPLAIN SELECT unknown(1)"):
            with self.assertRaises(Rejected):
                validate_sql(sql, "mysql")

    def test_selected_database_cannot_be_overridden(self):
        for sql in ("SELECT * FROM other.t", "SHOW TABLES FROM other", "DESCRIBE other.t", "SHOW COLUMNS FROM t FROM other"):
            with self.subTest(sql=sql), self.assertRaises(Rejected):
                validate_sql(sql, "mysql", "selected")
        self.assertEqual(validate_sql("SELECT * FROM `selected`.`t`", "mysql", "selected"), "select")
        self.assertEqual(validate_sql("SELECT * FROM information_schema.COLUMNS", "mysql", "selected"), "select")

    def test_custom_types_operators_and_lock_hints(self):
        for sql in ("SELECT 1 OPERATOR(public.+) 2", "SELECT CAST('payload' AS public.evil)"):
            with self.subTest(sql=sql), self.assertRaises(Rejected):
                validate_sql(sql, "postgresql")
        for sql in ("SELECT * FROM t WITH (UPDLOCK,HOLDLOCK)", "SELECT * FROM t WITH (TABLOCKX)", "SELECT * FROM t WITH (XLOCK)"):
            with self.subTest(sql=sql), self.assertRaises(Rejected):
                validate_sql(sql, "mssql")
        self.assertEqual(validate_sql("SELECT CAST('1' AS INTEGER)", "postgresql"), "select")


class RoutingTests(unittest.TestCase):
    def setUp(self):
        self.mysql = Connection("local", URL.create("mysql+pymysql", host="localhost", port=3306), ("database_readonly",))
        self.pg = Connection("pg", URL.create("postgresql+psycopg", host="db", port=5432, database="warehouse"))

    def test_jdbc_types(self):
        cases = {"jdbc:mysql://localhost:3306/db-a?useSSL=false": ("mysql", "db-a"),
                 "jdbc:mariadb://db:3306/test": ("mariadb", "test"),
                 "jdbc:postgresql://db:5432/warehouse": ("postgresql", "warehouse"),
                 "jdbc:sqlserver://db:1433;databaseName=wms;encrypt=true": ("mssql", "wms"),
                 "jdbc:oracle:thin:@//db:1521/ORCL": ("oracle", "ORCL")}
        for url, expected in cases.items():
            target = target_from_url(url)
            self.assertEqual((target.dialect, target.database), expected)

    def test_unique_target(self):
        selected = resolve([self.mysql, self.pg], datasource_url="jdbc:mysql://localhost:3306/db-a")
        self.assertEqual(selected.url.database, "db-a")
        self.assertEqual(resolve([self.mysql, self.pg], connection="database_readonly").name, "local")
        self.assertEqual(resolve([self.mysql, self.pg], dialect="mysql").name, "local")

    def test_reject_ambiguity_and_conflict(self):
        for kwargs in ({}, {"connection": "pg", "datasource_url": "jdbc:mysql://localhost:3306/x"}, {"datasource_url": "jdbc:postgresql://db:5432/wrong"}, {"datasource_url": "jdbc:mysql://localhost:3306/a", "database": "b"}):
            with self.assertRaises(Rejected):
                resolve([self.mysql, self.pg], **kwargs)

    def test_allowlist(self):
        restricted = Connection("restricted", self.mysql.url, allowed_databases=("a",))
        with self.assertRaises(Rejected):
            resolve([restricted], database="b")

    def test_profile_and_druid(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "application.yml").write_text("spring:\n  profiles:\n    active: dev\n  datasource:\n    url: jdbc:mysql://localhost:3306/base\n", encoding="utf-8")
            (root / "application-dev.yml").write_text("spring:\n  datasource:\n    url: jdbc:mysql://localhost:3306/dev\n", encoding="utf-8")
            (root / "application-prod.yml").write_text("spring:\n  datasource:\n    url: jdbc:mysql://remote:3306/prod\n", encoding="utf-8")
            self.assertEqual(project_targets(folder)[0].database, "dev")
            self.assertEqual(project_targets(folder, "prod")[0].host, "remote")
            (root / "application-dev.yml").write_text("spring:\n  datasource:\n    url: jdbc:mysql://localhost:3306/dev\n    druid:\n      master:\n        url: jdbc:mysql://localhost:3306/other\n", encoding="utf-8")
            self.assertEqual(resolve([self.mysql], project=folder).url.database, "other")

    def test_secrets_not_public_and_unknown_config(self):
        entry = Connection("a", URL.create("mysql+pymysql", username="secret-user", password="secret-password", host="localhost"))
        self.assertNotIn("secret", json.dumps(entry.public()))
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "connections.json"
            path.write_text(json.dumps({"connections": [{"name": "a", "url": "mysql://user:pass@localhost/a"}]}))
            with self.assertRaises(Rejected):
                load_connections(path)
        with patch.dict(os.environ, {"ALLOW_DDL_OPERATION": "true"}):
            with self.assertRaises(Rejected):
                load_connections()

    def test_driver_overrides_rejected_before_connect(self):
        urls = ["mysql+pymysql://localhost/safe?init_command=DROP%20TABLE%20audit_log", "mysql+pymysql://localhost/safe?database=prod", "postgresql+psycopg://db/safe?host=other&dbname=prod", "mssql+pyodbc://db/safe?odbc_connect=another", "sqlite:///fixture.db?mode=rw", "oracle+oracledb://db/?dsn=other"]
        for url in urls:
            with self.subTest(url=url), self.assertRaises(Rejected):
                validate_url_options(make_url(url))
        validate_url_options(make_url("mssql+pyodbc://db/safe?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes"))

    def test_profile_document_activation_and_import(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "application.yml").write_text("spring:\n  profiles:\n    active: dev\n", encoding="utf-8")
            file = root / "application-dev.yml"
            file.write_text("spring:\n  datasource:\n    url: jdbc:mysql://localhost:3306/dev\n---\nspring:\n  config:\n    activate:\n      on-profile: prod\n  datasource:\n    url: jdbc:mysql://localhost:3306/prod\n", encoding="utf-8")
            self.assertEqual(project_targets(folder)[0].database, "dev")
            file.write_text("spring:\n  config:\n    import: secret-config.yml\n  datasource:\n    url: jdbc:mysql://localhost:3306/dev\n", encoding="utf-8")
            with self.assertRaises(Rejected):
                project_targets(folder)

    def test_properties_activation_import_and_multidoc(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "application.properties").write_text("spring.profiles.active=dev\nspring.datasource.url=jdbc:mysql://localhost:3306/base\n", encoding="utf-8")
            file = root / "application-dev.properties"
            file.write_text("spring.config.activate.on-profile=prod\nspring.datasource.url=jdbc:mysql://localhost:3306/prod\n", encoding="utf-8")
            self.assertEqual(project_targets(folder)[0].database, "base")
            file.write_text("spring.datasource.url=jdbc:mysql://localhost:3306/dev\n#---\nspring.config.activate.on-profile=prod\nspring.datasource.url=jdbc:mysql://localhost:3306/prod\n", encoding="utf-8")
            self.assertEqual(project_targets(folder)[0].database, "dev")
            for control in ("spring.config.import=external.properties", "spring.profiles.include=extra", "spring.profiles.group.dev=extra"):
                file.write_text(control + "\nspring.datasource.url=jdbc:mysql://localhost:3306/dev\n", encoding="utf-8")
                with self.subTest(control=control), self.assertRaises(Rejected):
                    project_targets(folder)


class AccountTests(unittest.TestCase):
    def test_grants(self):
        verify_mysql_grants([{
            "grants": "GRANT SELECT, SHOW DATABASES, SHOW VIEW, REPLICATION CLIENT ON *.* TO `r`@`localhost`",
        }])
        for grant in ("GRANT SELECT, INSERT ON *.* TO r", "GRANT SELECT ON *.* TO r WITH GRANT OPTION", "GRANT readonly_role TO r", "GRANT EXECUTE ON *.* TO r", "GRANT ALL PRIVILEGES ON *.* TO r"):
            with self.subTest(grant=grant), self.assertRaises(Rejected):
                verify_mysql_grants([{"grants": grant}])

    def test_error_redaction(self):
        self.assertNotIn("password", json.dumps(safe_error(RuntimeError("password=secret SELECT personal_data"))))

    def test_other_dialects_reject_effective_write_and_routine_permissions(self):
        class Result:
            def __init__(self, rows=(), scalar=False):
                self.rows, self.value = rows, scalar
            def first(self):
                return self.rows[0] if self.rows else None
            def scalar(self):
                return self.value
            def __iter__(self):
                return iter(self.rows)
        class FakeConnection:
            def __init__(self, dialect, unsafe=None):
                self.dialect, self.unsafe = dialect, unsafe
            def exec_driver_sql(self, sql):
                if "FROM pg_roles" in sql:
                    return Result([(False,) * 5])
                if "FROM pg_proc" in sql:
                    return Result(scalar=self.unsafe == "routine")
                if "IS_SRVROLEMEMBER" in sql:
                    return Result([(0,) * 4])
                if "fn_my_permissions(NULL,'DATABASE')" in sql:
                    return Result([("CONNECT",), ("SELECT",)])
                if "fn_my_permissions(NULL,'SERVER')" in sql:
                    return Result([("CONNECT SQL",)])
                if "FROM sys.columns" in sql:
                    return Result(scalar=int(self.unsafe == "column"))
                if "FROM SESSION_PRIVS" in sql:
                    return Result([("CREATE SESSION",)])
                if "FROM ALL_COL_PRIVS" in sql:
                    return Result([("UPDATE",)] if self.unsafe == "column" else [])
                return Result()
        for dialect, unsafe in (("postgresql", "routine"), ("mssql", "column"), ("oracle", "column")):
            backend = Backend(Connection("test", URL.create({"postgresql": "postgresql+psycopg", "mssql": "mssql+pyodbc", "oracle": "oracle+oracledb"}[dialect], host="db", database="test")))
            backend.verify_account(FakeConnection(dialect))
            with self.subTest(dialect=dialect), self.assertRaises(Rejected):
                backend.verify_account(FakeConnection(dialect, unsafe))


class SQLiteTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "fixture.db"
        with closing(sqlite3.connect(self.path)) as conn:
            conn.executescript("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT UNIQUE); INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');")
            conn.commit()
        self.backend = Backend(Connection("sqlite-test", URL.create("sqlite", database=str(self.path)), max_rows=2, timeout_seconds=1))

    def tearDown(self):
        self.tmp.cleanup()

    def test_binds_limits_and_no_write(self):
        self.assertEqual(self.backend.query("SELECT * FROM t WHERE id=:id", {"id": 2})["rows"], [{"id": 2, "name": "b"}])
        self.assertTrue(self.backend.query("SELECT * FROM t")["truncated"])
        for sql in ("DELETE FROM t", "PRAGMA user_version=10", "ATTACH DATABASE 'x.db' AS x", "SELECT load_extension('x')"):
            with self.assertRaises(Rejected):
                self.backend.query(sql)
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM t").fetchone()[0], 3)
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 0)

    def test_driver_readonly_even_without_guard(self):
        engine = self.backend.engine()
        try:
            with engine.connect() as conn, self.assertRaises(Exception):
                conn.exec_driver_sql("DELETE FROM t")
        finally:
            engine.dispose()

    def test_metadata_and_explain(self):
        self.assertEqual(self.backend.metadata("tables")["tables"], [{"name": "t", "type": "table"}])
        self.assertEqual(self.backend.metadata("describe", table="t")["primaryKey"]["constrained_columns"], ["id"])
        self.assertTrue(self.backend.query("EXPLAIN QUERY PLAN SELECT * FROM t")["rows"])

    def test_timeout(self):
        with self.assertRaises(Exception):
            self.backend.query("WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x) SELECT SUM(n) FROM x")

    def test_missing_file_not_created(self):
        file = self.path.parent / "missing.db"
        backend = Backend(Connection("missing", URL.create("sqlite", database=str(file))))
        with self.assertRaises(Rejected):
            backend.query("SELECT 1")
        self.assertFalse(file.exists())

    def test_tool_dispatch(self):
        result = run_tool("db_query", {"connection": "sqlite-test", "sql": "SELECT * FROM t"}, [self.backend.config])
        self.assertFalse(result.isError)
        self.assertTrue(result.structuredContent["truncated"])
        for args in ({"sql": "SELECT 1", "unsafe": True}, {"sql": "SELECT 1", "limit": "2"}):
            with self.assertRaises(Rejected):
                run_tool("db_query", args, [self.backend.config])


if __name__ == "__main__":
    unittest.main()
