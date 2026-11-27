import json
import unittest

from pymysql.err import OperationalError, ProgrammingError
from sqlalchemy.exc import OperationalError as SQLAlchemyOperationalError

from readonly_mcp.server import safe_error


class ErrorDiagnosticsTests(unittest.TestCase):
    def test_wrapped_mysql_error_preserves_errno_and_sqlstate_without_secrets(self):
        original = OperationalError(1044, "user=private password=secret database=private", sqlstate="42000")
        error = SQLAlchemyOperationalError("SELECT private_data", {"password": "secret"}, original)
        info = safe_error(error)
        self.assertEqual(info["errno"], 1044)
        self.assertEqual(info["code"], "42000")
        self.assertIn("SELECT 授权", info["error"])
        self.assertEqual(info["type"], "OperationalError")
        self.assertNotIn("private", json.dumps(info))
        self.assertNotIn("secret", json.dumps(info))

    def test_mysql_permission_syntax_and_session_errors_are_distinct(self):
        for errno, expected in ((1045, "身份验证"), (1049, "不存在"), (1064, "语法"),
                                (1142, "命令"), (1143, "字段"), (1193, "会话变量"), (1227, "权限")):
            with self.subTest(errno=errno):
                error = ProgrammingError(errno, "sensitive raw message", sqlstate="42000")
                info = safe_error(error)
                self.assertEqual(info["errno"], errno)
                self.assertIn(expected, info["error"])
                self.assertNotIn("sensitive", json.dumps(info))

    def test_old_driver_without_sqlstate_keeps_numeric_code(self):
        info = safe_error(OperationalError(1044, "sensitive"))
        self.assertEqual(info["errno"], 1044)
        self.assertEqual(info["code"], "1044")

    def test_unknown_errors_and_untrusted_codes_do_not_leak_messages(self):
        for error in (RuntimeError("password=secret"), OperationalError(9999, "password=secret"),
                      OperationalError("password=secret", sqlstate="password=secret")):
            with self.subTest(error_type=type(error).__name__):
                info = safe_error(error)
                self.assertNotIn("secret", json.dumps(info))
                self.assertEqual(info["error"], "数据库操作失败")


if __name__ == "__main__":
    unittest.main()
