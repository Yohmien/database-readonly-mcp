from __future__ import annotations

import base64
import datetime
import decimal
import re
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.pool import NullPool

from .config import Connection, Rejected, validate_url_options
from .guard import PURE_FUNCTIONS, validate_sql, referenced_databases


def verify_mysql_grants(rows):
    allowed = {"USAGE", "SELECT", "SHOW DATABASES", "SHOW VIEW", "REPLICATION CLIENT"}
    if not rows:
        raise Rejected("Cannot verify MySQL grants")
    for row in rows:
        for value in row.values():
            match = re.fullmatch(r"GRANT\s+(.+?)\s+ON\s+.+?\s+TO\s+.+", str(value), re.I)
            if not match or "WITH GRANT OPTION" in str(value).upper() or not {x.strip().upper() for x in match[1].split(",")} <= allowed:
                raise Rejected("MySQL account has unsafe or unverifiable grants/roles")


def serializable(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, bytes):
        return {"encoding": "base64", "value": base64.b64encode(value).decode("ascii")}
    if isinstance(value, (tuple, list)):
        return [serializable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): serializable(v) for k, v in value.items()}
    return str(value)


class Backend:
    def __init__(self, config: Connection):
        self.config = config
        self.dialect = config.target.dialect

    def engine(self):
        url = validate_url_options(self.config.url)
        options = {"poolclass": NullPool, "hide_parameters": True}
        if self.dialect == "sqlite":
            file = Path(self.config.target.database)
            if not file.is_file():
                raise Rejected("SQLite database file does not exist")
            def connect():
                raw = sqlite3.connect(file.as_uri() + "?mode=ro", uri=True, timeout=self.config.timeout_seconds)
                raw.enable_load_extension(False)
                raw.execute("PRAGMA query_only=ON")
                raw.execute("PRAGMA trusted_schema=OFF")
                return raw
            options["creator"] = connect
            url = "sqlite://"
        elif self.dialect in {"mysql", "mariadb"}:
            options["connect_args"] = {"connect_timeout": self.config.timeout_seconds, "read_timeout": self.config.timeout_seconds, "write_timeout": self.config.timeout_seconds, "client_flag": 0}
        elif self.dialect == "postgresql":
            options["connect_args"] = {"connect_timeout": self.config.timeout_seconds, "options": "-c default_transaction_read_only=on -c statement_timeout=" + str(self.config.timeout_seconds * 1000)}
        elif self.dialect == "mssql":
            # ApplicationIntent is routing only; effective permissions are checked below.
            options["connect_args"] = {"timeout": self.config.timeout_seconds}
        elif self.dialect == "oracle":
            options["connect_args"] = {"tcp_connect_timeout": self.config.timeout_seconds}
        engine = create_engine(url, **options)
        return engine

    def verify_account(self, conn):
        if self.dialect in {"mysql", "mariadb"}:
            rows = [dict(row) for row in conn.exec_driver_sql("SHOW GRANTS").mappings()]
            verify_mysql_grants(rows)
            return rows
        if self.dialect == "sqlite":
            if conn.exec_driver_sql("PRAGMA query_only").scalar() != 1:
                raise Rejected("SQLite query_only not active")
            return []
        if self.dialect == "postgresql":
            role = conn.exec_driver_sql("SELECT rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls FROM pg_roles WHERE rolname=current_user").first()
            if role is None or any(role):
                raise Rejected("PostgreSQL privileged account is forbidden")
            unsafe = conn.exec_driver_sql("""SELECT EXISTS (
                SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname NOT IN ('pg_catalog','information_schema')
                AND c.relkind IN ('r','p','v','m','f') AND
                (pg_has_role(current_user,c.relowner,'USAGE') OR
                 has_table_privilege(c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER') OR
                 has_any_column_privilege(c.oid,'INSERT,UPDATE'))) OR EXISTS (
                SELECT 1 FROM pg_namespace WHERE nspname NOT LIKE 'pg_%%'
                AND (has_schema_privilege(oid,'CREATE') OR pg_has_role(current_user,nspowner,'USAGE')))
                OR has_database_privilege(current_database(),'CREATE')
                OR EXISTS (SELECT 1 FROM pg_database WHERE datname=current_database()
                           AND pg_has_role(current_user,datdba,'USAGE'))""").scalar()
            if unsafe:
                raise Rejected("PostgreSQL write/ownership/schema-create privileges are forbidden")
            executable_routine = conn.exec_driver_sql("""SELECT EXISTS (
                SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                WHERE n.nspname NOT IN ('pg_catalog','information_schema')
                AND has_function_privilege(p.oid,'EXECUTE'))""").scalar()
            if executable_routine:
                raise Rejected("PostgreSQL account can execute non-system routines; function overloads/side effects cannot be verified")
            return []
        if self.dialect == "mssql":
            admin = conn.exec_driver_sql("SELECT IS_SRVROLEMEMBER('sysadmin'), IS_MEMBER('db_owner'), IS_MEMBER('db_ddladmin'), IS_MEMBER('db_datawriter')").first()
            if admin is None or any(v is None or v == 1 for v in admin):
                raise Rejected("SQL Server privileged or unverifiable account")
            permissions = {row[0].upper() for row in conn.exec_driver_sql("SELECT permission_name FROM sys.fn_my_permissions(NULL,'DATABASE')")}
            if not permissions or not permissions <= {"CONNECT", "SELECT", "VIEW DEFINITION", "VIEW DATABASE STATE", "VIEW DATABASE PERFORMANCE STATE", "VIEW DATABASE SECURITY STATE"}:
                raise Rejected("SQL Server unsafe database permissions")
            unsafe = conn.exec_driver_sql("""SELECT COUNT(*) FROM sys.objects
                WHERE is_ms_shipped=0 AND (HAS_PERMS_BY_NAME(QUOTENAME(SCHEMA_NAME(schema_id))+'.'+QUOTENAME(name),'OBJECT','INSERT')=1
                OR HAS_PERMS_BY_NAME(QUOTENAME(SCHEMA_NAME(schema_id))+'.'+QUOTENAME(name),'OBJECT','UPDATE')=1
                OR HAS_PERMS_BY_NAME(QUOTENAME(SCHEMA_NAME(schema_id))+'.'+QUOTENAME(name),'OBJECT','DELETE')=1
                OR HAS_PERMS_BY_NAME(QUOTENAME(SCHEMA_NAME(schema_id))+'.'+QUOTENAME(name),'OBJECT','ALTER')=1
                OR HAS_PERMS_BY_NAME(QUOTENAME(SCHEMA_NAME(schema_id))+'.'+QUOTENAME(name),'OBJECT','CONTROL')=1
                OR HAS_PERMS_BY_NAME(QUOTENAME(SCHEMA_NAME(schema_id))+'.'+QUOTENAME(name),'OBJECT','EXECUTE')=1)""").scalar()
            if unsafe:
                raise Rejected("SQL Server unsafe object permissions")
            # Schema grants can create/alter objects even without table-level grants.
            unsafe_schema = conn.exec_driver_sql("""SELECT COUNT(*) FROM sys.schemas WHERE
                HAS_PERMS_BY_NAME(name,'SCHEMA','ALTER')=1 OR HAS_PERMS_BY_NAME(name,'SCHEMA','CONTROL')=1
                OR HAS_PERMS_BY_NAME(name,'SCHEMA','INSERT')=1 OR HAS_PERMS_BY_NAME(name,'SCHEMA','UPDATE')=1
                OR HAS_PERMS_BY_NAME(name,'SCHEMA','DELETE')=1 OR HAS_PERMS_BY_NAME(name,'SCHEMA','EXECUTE')=1""").scalar()
            if unsafe_schema:
                raise Rejected("SQL Server unsafe schema permissions")
            server_permissions = {row[0].upper() for row in conn.exec_driver_sql("SELECT permission_name FROM sys.fn_my_permissions(NULL,'SERVER')")}
            if not server_permissions <= {"CONNECT SQL", "VIEW ANY DATABASE", "VIEW ANY DEFINITION", "VIEW SERVER STATE", "VIEW SERVER PERFORMANCE STATE", "VIEW SERVER SECURITY STATE"}:
                raise Rejected("SQL Server unsafe server permissions")
            unsafe_columns = conn.exec_driver_sql("""SELECT COUNT(*) FROM sys.columns c JOIN sys.objects o ON o.object_id=c.object_id
                WHERE o.is_ms_shipped=0 AND (
                HAS_PERMS_BY_NAME(QUOTENAME(SCHEMA_NAME(o.schema_id))+'.'+QUOTENAME(o.name),'OBJECT','INSERT',c.name,'COLUMN')=1
                OR HAS_PERMS_BY_NAME(QUOTENAME(SCHEMA_NAME(o.schema_id))+'.'+QUOTENAME(o.name),'OBJECT','UPDATE',c.name,'COLUMN')=1)""").scalar()
            if unsafe_columns:
                raise Rejected("SQL Server column write permissions")
            return []
        if self.dialect == "oracle":
            permissions = {row[0].upper() for row in conn.exec_driver_sql("SELECT PRIVILEGE FROM SESSION_PRIVS")}
            if not permissions <= {"CREATE SESSION", "SELECT ANY TABLE", "READ ANY TABLE", "SELECT ANY DICTIONARY"}:
                raise Rejected("Oracle unsafe system privileges")
            # Recursively include enabled roles; unknown EXECUTE rights fail closed.
            grants = conn.exec_driver_sql("""SELECT PRIVILEGE FROM ALL_TAB_PRIVS WHERE GRANTEE IN (USER,'PUBLIC')
                OR GRANTEE IN (SELECT ROLE FROM SESSION_ROLES)""")
            if any(row[0].upper() not in {"SELECT", "READ"} for row in grants):
                raise Rejected("Oracle unsafe object/role/PUBLIC privileges; dedicated restricted account required")
            column_grants = conn.exec_driver_sql("""SELECT PRIVILEGE FROM ALL_COL_PRIVS WHERE GRANTEE IN (USER,'PUBLIC')
                OR GRANTEE IN (SELECT ROLE FROM SESSION_ROLES)""")
            if any(row[0].upper() not in {"SELECT", "READ"} for row in column_grants):
                raise Rejected("Oracle column write privileges")
            owned = conn.exec_driver_sql("SELECT COUNT(*) FROM USER_OBJECTS WHERE OBJECT_TYPE IN ('TABLE','VIEW','PROCEDURE','FUNCTION','PACKAGE','TRIGGER','SEQUENCE')").scalar()
            if owned:
                raise Rejected("Oracle object-owner account is forbidden")
            return []
        raise Rejected("No readonly account verifier for this dialect")

    @contextmanager
    def session(self):
        engine = self.engine()
        try:
            with engine.connect() as conn:
                raw = conn.connection.driver_connection
                if self.dialect == "oracle":
                    raw.call_timeout = self.config.timeout_seconds * 1000
                elif self.dialect == "mssql":
                    raw.timeout = self.config.timeout_seconds
                grants = self.verify_account(conn)
                conn.rollback()
                if self.dialect in {"mysql", "mariadb"}:
                    if self.dialect == "mysql":
                        conn.exec_driver_sql("SET SESSION MAX_EXECUTION_TIME=" + str(self.config.timeout_seconds * 1000))
                    else:
                        conn.exec_driver_sql("SET SESSION max_statement_time=" + str(self.config.timeout_seconds))
                    conn.exec_driver_sql("SET SESSION TRANSACTION READ ONLY")
                    conn.rollback()
                elif self.dialect == "postgresql":
                    conn.exec_driver_sql("SET TRANSACTION READ ONLY")
                    conn.exec_driver_sql("SET LOCAL search_path=pg_catalog,public")
                elif self.dialect == "oracle":
                    conn.exec_driver_sql("SET TRANSACTION READ ONLY")
                elif self.dialect == "sqlite":
                    deadline = time.monotonic() + self.config.timeout_seconds
                    raw.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
                    def authorizer(action, arg1, arg2, db, trigger):
                        if action == sqlite3.SQLITE_FUNCTION and (arg2 or "").upper() not in PURE_FUNCTIONS:
                            return sqlite3.SQLITE_DENY
                        return sqlite3.SQLITE_OK if action in {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION, sqlite3.SQLITE_RECURSIVE} else sqlite3.SQLITE_DENY
                    # Inspector PRAGMAs run only in the internal metadata context below.
                    raw.set_authorizer(authorizer)
                try:
                    yield conn, grants
                finally:
                    if self.dialect == "sqlite":
                        raw.set_authorizer(None)
                        raw.set_progress_handler(None, 0)
                    conn.rollback()
        finally:
            engine.dispose()

    def query(self, sql, params=None, limit=None):
        validate_sql(sql, self.dialect, self.config.target.database)
        if self.config.allowed_databases and self.dialect in {'mysql', 'mariadb'}:
            referenced = referenced_databases(sql, self.dialect)
            if referenced - set(self.config.allowed_databases) - {'information_schema'}:
                raise Rejected('SQL references a database outside allowed_databases')
        cap = self.config.max_rows if limit is None else limit
        if not isinstance(cap, int) or isinstance(cap, bool) or not 1 <= cap <= self.config.max_rows:
            raise Rejected("limit must be between 1 and configured max_rows")
        if params is not None and (not isinstance(params, dict) or any(not isinstance(v, (str, int, float, bool, type(None))) for v in params.values())):
            raise Rejected("params must contain scalar named bind values")
        started = time.monotonic()
        streaming = self.dialect in {"mysql", "mariadb", "postgresql"}
        if streaming and self.dialect == "postgresql" and re.match(r"\s*EXPLAIN\b", sql, re.I):
            # PostgreSQL 会为 stream_results 发出 DECLARE ... CURSOR FOR <sql>，
            # 而 DECLARE CURSOR 不接受 EXPLAIN，会报 42601 语法错误。
            # MySQL/MariaDB 的服务端游标直接执行语句、不使用 DECLARE，故保持不变。
            streaming = False
        with self.session() as (conn, _):
            result = conn.execution_options(stream_results=streaming, max_row_buffer=cap + 1).execute(text(sql), params or {})
            try:
                columns = list(result.keys())
                raw_rows = result.fetchmany(cap + 1)
                # Keep original column positions and make object keys collision-free.
                used, row_keys = set(), []
                reserved = set(columns)
                for column in columns:
                    key, suffix = column, 2
                    while key in used or (key != column and key in reserved):
                        key = f'{column}__{suffix}'
                        suffix += 1
                    used.add(key)
                    row_keys.append(key)
                rows = [dict(zip(row_keys, map(serializable, row))) for row in raw_rows[:cap]]
                values = [[serializable(value) for value in row] for row in raw_rows[:cap]]
                output = {"connection": self.config.name, "database": self.config.target.database,
                        "columns": columns, "rows": rows, "values": values, "count": len(rows),
                        "truncated": len(raw_rows) > cap, "elapsedMs": round((time.monotonic() - started) * 1000, 2)}
                if row_keys != columns:
                    output['rowKeys'] = row_keys
                return output
            finally:
                result.close()

    def metadata(self, operation, schema=None, table=None, pattern=None, limit=None, detail="full", offset=0):
        limit = min(100, self.config.max_rows) if limit is None else limit
        if not 1 <= limit <= min(1000, self.config.max_rows):
            raise Rejected("Invalid metadata limit")
        if detail not in {"columns", "full"}:
            raise Rejected("Invalid metadata detail")
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise Rejected('offset must be a nonnegative integer')
        if self.config.allowed_databases and self.dialect in {'mysql', 'mariadb'} and schema and schema not in self.config.allowed_databases:
            raise Rejected('Metadata schema is outside allowed_databases')
        if self.dialect in {"mysql", "mariadb"} and schema and self.config.target.database and schema != self.config.target.database:
            raise Rejected("Metadata schema conflicts with selected database")
        if self.dialect == "sqlite" and schema not in {None, "main"}:
            raise Rejected("Only the registered SQLite file's main schema is allowed")
        with self.session() as (conn, _):
            if self.dialect == "sqlite":
                # SQLAlchemy emits fixed introspection PRAGMAs; no caller SQL in this path.
                conn.connection.driver_connection.set_authorizer(None)
            inspector = inspect(conn)
            if operation == "schemas":
                names = sorted(inspector.get_schema_names())
                if self.config.allowed_databases and self.dialect in {'mysql', 'mariadb'}:
                    names = [name for name in names if name in self.config.allowed_databases]
                truncated = offset + limit < len(names)
                return {"schemas": names[offset:offset+limit], "truncated": truncated,
                        'offset': offset, 'nextOffset': offset + limit if truncated else None}
            if operation == "tables":
                items = [{"name": name, "type": kind} for kind, names in (("table", inspector.get_table_names(schema)), ("view", inspector.get_view_names(schema))) for name in names if pattern is None or pattern.casefold() in name.casefold()]
                items.sort(key=lambda x: (x["name"], x["type"]))
                truncated = offset + limit < len(items)
                return {"tables": items[offset:offset+limit], "truncated": truncated,
                        'offset': offset, 'nextOffset': offset + limit if truncated else None}
            if not table or not inspector.has_table(table, schema):
                raise Rejected("Table/view does not exist in the selected schema")
            result = {"connection": self.config.name, "database": self.config.target.database,
                      "schema": schema, "table": table, "columns": inspector.get_columns(table, schema)}
            if detail == "full":
                result.update(primaryKey=inspector.get_pk_constraint(table, schema),
                              foreignKeys=inspector.get_foreign_keys(table, schema),
                              indexes=inspector.get_indexes(table, schema),
                              uniqueConstraints=inspector.get_unique_constraints(table, schema))
            return serializable(result)
