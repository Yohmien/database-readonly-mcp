from __future__ import annotations

import re
import logging

import sqlglot
from sqlglot import exp

from .config import Rejected

logging.getLogger("sqlglot").setLevel(logging.CRITICAL)


# Only known pure scalar/aggregate functions. Unknown UDFs are intentionally rejected.
PURE_FUNCTIONS = set("ABS ACOS ASIN ATAN ATAN2 AVG BIT_AND BIT_OR BIT_XOR CEIL CEILING CHAR_LENGTH CHARACTER_LENGTH COALESCE CONCAT CONCAT_WS COUNT CUME_DIST CURRENT_DATE CURRENT_TIME CURRENT_TIMESTAMP CURDATE CURTIME DATE DATE_ADD DATE_DIFF DATE_FORMAT DATE_SUB DATEDIFF DAY DAYOFMONTH DAYOFWEEK DAYOFYEAR DENSE_RANK EXTRACT FIRST_VALUE FLOOR FORMAT GREATEST GROUP_CONCAT HEX IF IFNULL IIF ISNULL JSON_ARRAY JSON_EXTRACT JSON_LENGTH JSON_OBJECT JSON_UNQUOTE LAG LAST_DAY LAST_VALUE LEAD LEAST LEFT LENGTH LN LOG LOG10 LOWER LPAD LTRIM MAX MD5 MIN MOD MONTH NOW NTH_VALUE NTILE NULLIF OCTET_LENGTH PERCENT_RANK POSITION POW POWER RANK REGEXP_LIKE REGEXP_REPLACE REPLACE REVERSE RIGHT ROUND ROW_NUMBER RPAD RTRIM SIGN SIN SQRT STDDEV STDDEV_POP STDDEV_SAMP STR_TO_DATE STRING_AGG SUBSTR SUBSTRING SUM TAN TIMESTAMP TIMESTAMPDIFF TO_CHAR TO_DATE TO_TIMESTAMP TRIM TRUNC UCASE UPPER VAR_POP VAR_SAMP WEEK YEAR CAST TRY_CAST CONVERT DECODE NVL NVL2 DATABASE SCHEMA CURRENT_SCHEMA CURRENT_USER USER VERSION SQLITE_VERSION TYPEOF INSTR JULIANDAY STRFTIME UNIXEPOCH DATETIME TIME ILIKE LIKE BETWEEN IN AND OR NOT CASE IF DATE_TRUNC TS_OR_DS_ADD TS_OR_DS_DIFF TIME_TO_STR STR_TO_TIME STR_TO_DATE TIME_STR_TO_DATE STR_POSITION JSON_EXTRACT_SCALAR ARRAY_AGG ARRAY_SIZE ARRAY_TO_STRING".split())
FORBIDDEN_TYPES = {"Insert", "Update", "Delete", "Merge", "Create", "Drop", "Alter", "TruncateTable", "Command", "Into", "Lock", "Transaction", "Commit", "Rollback", "Set", "Use", "Grant", "Revoke", "Execute", "Copy", "Attach", "Detach", "Pragma", "LoadData", "Assignment", "PropertyEQ"}
PURE_FUNCTIONS.update({'UNIX_TIMESTAMP', 'UNIX_TO_TIME', 'FROM_UNIXTIME', 'FIELD', 'FIND_IN_SET', 'JSON_ARRAYAGG'})
IDENTIFIER = r'(?:`(?:[^`]|``)+`|"(?:[^"]|"")+"|\[(?:[^\]]|\]\])+\]|[A-Za-z_][A-Za-z0-9_$-]*)'


def referenced_databases(sql: str, dialect: str) -> set[str]:
    if dialect not in {"mysql", "mariadb"}:
        return set()
    if re.match(r'\s*EXPLAIN\b', sql, re.I):
        body = re.sub(r'^\s*EXPLAIN\s+(?:FORMAT\s*=\s*JSON\s+)?', '', sql, flags=re.I)
        return referenced_databases(body, dialect) if body != sql else set()
    try:
        tree = sqlglot.parse_one(sql, read="mysql", error_level=sqlglot.ErrorLevel.RAISE)
        databases = {node.db for node in tree.find_all(exp.Table) if node.db}
        if databases or isinstance(tree, (exp.Select, exp.Union, exp.Intersect, exp.Except)):
            return databases
    except Exception:
        pass
    if not re.match(r"\s*(SHOW|DESC|DESCRIBE)\b", sql, re.I):
        return set()
    # Validated metadata grammar only: match qualified names or a FROM/IN schema.
    def unquote(value):
        return value[1:-1].replace("``", "`") if value.startswith("`") else value
    qualified = {unquote(m[1]) for m in re.finditer(rf"({IDENTIFIER})\s*\.\s*{IDENTIFIER}", sql)}
    if qualified:
        return qualified
    tables = re.fullmatch(rf"SHOW\s+(?:FULL\s+)?TABLES\s+(?:FROM|IN)\s+({IDENTIFIER});?", sql.strip(), re.I)
    if tables:
        return {unquote(tables[1])}
    columns = re.fullmatch(rf"SHOW\s+(?:FULL\s+)?(?:COLUMNS|INDEX|INDEXES|KEYS)\s+(?:FROM|IN)\s+{IDENTIFIER}\s+(?:FROM|IN)\s+({IDENTIFIER});?", sql.strip(), re.I)
    return {unquote(columns[1])} if columns else set()


def validate_sql(sql: str, dialect: str, database: str | None = None) -> str:
    if not isinstance(sql, str) or not sql.strip() or len(sql) > 100000:
        raise Rejected("SQL must be nonempty and at most 100000 characters")
    # Reject executable comments even when nested inside a quoted literal: fail closed.
    if "/*!" in sql or "/*M!" in sql.upper():
        raise Rejected("Executable SQL comments are forbidden")
    sql = sql.strip()
    if sql.endswith(";"):
        sql = sql[:-1].rstrip()
    if sql.endswith(";"):
        raise Rejected("Only one optional terminal semicolon is allowed")
    if database and dialect in {"mysql", "mariadb"}:
        referenced = referenced_databases(sql, dialect)
        if referenced - {database, "information_schema"}:
            raise Rejected("SQL references a database outside the selected target")
    if dialect in {"mysql", "mariadb"} and re.fullmatch(r"SHOW\s+GRANTS", sql, re.I):
        return "grants"
    # Metadata-only grammar avoids parser gaps without a free-form fallback.
    if dialect in {"mysql", "mariadb"}:
        patterns = [r"SHOW\s+(?:DATABASES|SCHEMAS)",
                    rf"SHOW\s+(?:FULL\s+)?TABLES(?:\s+(?:FROM|IN)\s+{IDENTIFIER})?",
                    rf"SHOW\s+(?:FULL\s+)?COLUMNS\s+(?:FROM|IN)\s+{IDENTIFIER}(?:\.{IDENTIFIER})?(?:\s+(?:FROM|IN)\s+{IDENTIFIER})?",
                    rf"SHOW\s+(?:INDEX|INDEXES|KEYS)\s+(?:FROM|IN)\s+{IDENTIFIER}(?:\.{IDENTIFIER})?(?:\s+(?:FROM|IN)\s+{IDENTIFIER})?",
                    rf"SHOW\s+CREATE\s+(?:TABLE|VIEW)\s+{IDENTIFIER}(?:\.{IDENTIFIER})?",
                    rf"(?:DESCRIBE|DESC)\s+{IDENTIFIER}(?:\.{IDENTIFIER})?"]
        if any(re.fullmatch(pattern, sql, re.I) for pattern in patterns):
            return "metadata"
    if re.match(r"EXPLAIN\b", sql, re.I):
        body = re.sub(r"^EXPLAIN\s+(?:FORMAT\s*=\s*JSON\s+)?", "", sql, flags=re.I)
        if dialect not in {"mysql", "mariadb", "postgresql", "sqlite"} or body == sql or re.match(r"(?:ANALYZE|ANALYSE)\b", body, re.I):
            raise Rejected("Only non-executing EXPLAIN SELECT/WITH is allowed")
        if dialect == "sqlite":
            body = re.sub(r"^QUERY\s+PLAN\s+", "", body, flags=re.I)
        validate_sql(body, dialect)
        if not re.match(r"(?:SELECT|WITH)\b", body, re.I):
            raise Rejected("EXPLAIN must wrap SELECT/WITH")
        return "explain"
    try:
        statements = sqlglot.parse(sql, read={"mariadb": "mysql", "mssql": "tsql", "postgresql": "postgres"}.get(dialect, dialect), error_level=sqlglot.ErrorLevel.RAISE)
    except Exception:
        raise Rejected("SQL cannot be safely parsed for this dialect") from None
    if len(statements) != 1 or not isinstance(statements[0], (exp.Select, exp.Union, exp.Intersect, exp.Except)):
        raise Rejected("Only one SELECT/WITH query or supported metadata statement is allowed")
    for node in statements[0].walk():
        if type(node).__name__ in FORBIDDEN_TYPES:
            raise Rejected("SQL contains a write, locking or state-changing expression")
        if type(node).__name__ == "Operator":
            raise Rejected("Custom operators are forbidden")
        if isinstance(node, exp.DataType) and node.this in {exp.DataType.Type.USERDEFINED, exp.DataType.Type.UNKNOWN}:
            raise Rejected("Custom/unknown type conversions are forbidden")
        if isinstance(node, exp.WithTableHint):
            if any(hint.name.upper() not in {"NOLOCK", "READUNCOMMITTED"} for hint in node.expressions):
                raise Rejected("Locking/unsupported table hints are forbidden")
        if isinstance(node, exp.Table) and node.catalog:
            raise Rejected("Cross-catalog/linked-server queries are forbidden")
        if isinstance(node, exp.Dot) and isinstance(node.expression, exp.Func):
            raise Rejected("Schema-qualified functions are forbidden")
        if isinstance(node, exp.Func):
            name = (node.name if isinstance(node, exp.Anonymous) else node.sql_name()).upper()
            if name not in PURE_FUNCTIONS:
                raise Rejected("Function is not in the pure-read allowlist: " + name)
    return "select"
