"""Offline checks backed by the existing SQLGlot parser."""
from __future__ import annotations

import re

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from .config import Rejected


def check_sql(sql, dialect):
    if not sql.strip() or len(sql) > 1_000_000:
        raise Rejected("SQL 必须非空且不超过 1000000 字符")
    dialect = {"mariadb": "mysql", "mssql": "tsql", "postgresql": "postgres"}.get(dialect, dialect)
    if dialect == "mysql" and (
        re.search(r"(?im)^\s*delimiter\b", sql)
        or re.search(r"(?is)\bcreate\b.*?\b(?:procedure|function|trigger|event)\b", sql)
    ):
        return {
            "tool": "SQLGlot",
            "version": sqlglot.__version__,
            "status": "incomplete",
            "recommendedTool": "db_script_test",
            "diagnostics": [],
            "message": "检测到 MySQL 客户端脚本语法；SQLGlot 不解析 DELIMITER 与存储对象包装，请用 db_script_test 在镜像中执行两遍",
        }
    try:
        trees = sqlglot.parse(sql, read=dialect, error_level=sqlglot.ErrorLevel.RAISE)
    except ParseError as error:
        return {"tool": "SQLGlot", "version": sqlglot.__version__, "status": "failed",
                "diagnostics": [{"line": e.get("line"), "column": e.get("col"),
                                 "message": e.get("description", "SQL 语法解析失败")} for e in error.errors],
                "message": "静态解析失败；未连接数据库"}
    unsupported, risks = [], []
    for index, tree in enumerate(trees, 1):
        if tree is None:
            continue
        if isinstance(tree, (exp.Column, exp.Literal, exp.Alias)) or any(isinstance(node, exp.Command) for node in tree.walk()):
            unsupported.append(index)
        for node in tree.walk():
            kind = type(node).__name__
            if kind in {"Drop", "TruncateTable", "Delete", "Merge"}:
                risks.append({"statement": index, "kind": kind, "message": "删除或覆盖数据，需镜像执行验证"})
            elif isinstance(node, exp.Update) and not node.args.get("where"):
                risks.append({"statement": index, "kind": kind, "message": "无 WHERE 的更新，需镜像执行验证"})
            elif kind in {"AlterColumn", "DropColumn", "ModifyColumn"}:
                risks.append({"statement": index, "kind": kind, "message": "破坏性或类型变更 DDL，需镜像执行验证"})
    return {"tool": "SQLGlot", "version": sqlglot.__version__,
            "status": "incomplete" if unsupported or not any(t is not None for t in trees) else "passed", "statements": sum(t is not None for t in trees),
            "unsupportedStatements": unsupported, "risks": risks,
            "message": "未支持语法需人工核对；未连接数据库" if unsupported else "静态解析通过；不证明对象存在、幂等性或运行结果"}
