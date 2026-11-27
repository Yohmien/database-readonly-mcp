import tempfile
import sqlite3
import unittest
from pathlib import Path
from contextlib import closing, contextmanager
from unittest.mock import patch

from sqlalchemy.engine import URL
from readonly_mcp.config import Connection, Rejected, resolve, project_targets
from readonly_mcp.backend import Backend
from readonly_mcp.guard import validate_sql
from readonly_mcp.server import run_tool


class DefectTests(unittest.TestCase):
    def test_allowlist_applies_before_connection(self):
        entry = Connection('restricted', URL.create('mysql+pymysql', host='localhost'), allowed_databases=('safe',))
        backend = Backend(entry)
        for sql in ('SELECT * FROM other.t', 'EXPLAIN SELECT * FROM other.t', 'SHOW TABLES FROM other'):
            with self.subTest(sql=sql), patch.object(backend, 'session') as session, self.assertRaises(Rejected):
                backend.query(sql)
            session.assert_not_called()
        with self.assertRaises(Rejected):
            resolve([entry], database='other')
        fixed = Connection('fixed', entry.url.set(database='other'), allowed_databases=('safe',))
        with self.assertRaises(Rejected):
            resolve([fixed], database='other')
        with self.assertRaises(Rejected):
            backend.metadata('tables', schema='other')

    def test_default_and_explicit_active_profiles(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root/'application.yml').write_text('spring:\n  datasource:\n    url: jdbc:mysql://localhost/base\n', encoding='utf-8')
            (root/'application-default.yml').write_text('spring:\n  datasource:\n    url: jdbc:mysql://localhost/default_db\n', encoding='utf-8')
            (root/'application-prod.yml').write_text('spring:\n  datasource:\n    url: jdbc:mysql://localhost/prod_db\n', encoding='utf-8')
            self.assertEqual(project_targets(folder)[0].database, 'default_db')
            (root/'application.yml').write_text('spring:\n  profiles:\n    active: prod\n', encoding='utf-8')
            self.assertEqual(project_targets(folder)[0].database, 'prod_db')
            self.assertEqual(project_targets(folder, 'default')[0].database, 'default_db')

    def test_verified_builtin_functions(self):
        for sql in ('SELECT UNIX_TIMESTAMP()', 'SELECT FROM_UNIXTIME(0)', 'SELECT FIELD(a,1,2) FROM t', 'SELECT FIND_IN_SET(a,b) FROM t', 'SELECT JSON_ARRAYAGG(a) FROM t'):
            self.assertEqual(validate_sql(sql, 'mysql'), 'select')
        for sql in ("SELECT SLEEP(1)", "SELECT GET_LOCK('x',1)", 'SELECT arbitrary_udf()'):
            with self.assertRaises(Rejected):
                validate_sql(sql, 'mysql')

    def test_metadata_all_pages(self):
        class Inspector:
            def get_table_names(self, schema):
                return ['t'+str(n).zfill(4) for n in reversed(range(1105))]
            def get_view_names(self, schema):
                return []
            def get_schema_names(self):
                return ['s'+str(n).zfill(4) for n in reversed(range(1105))]
        @contextmanager
        def session():
            yield object(), []
        backend = Backend(Connection('test', URL.create('mysql+pymysql', host='localhost')))
        with patch.object(backend,'session',session), patch('readonly_mcp.backend.inspect',return_value=Inspector()):
            for operation, key in [('tables','tables'), ('schemas','schemas')]:
                values, offset = [], 0
                while True:
                    result = backend.metadata(operation, limit=100, offset=offset)
                    values.extend(result[key])
                    if result['nextOffset'] is None:
                        self.assertFalse(result['truncated'])
                        break
                    self.assertTrue(result['truncated'])
                    offset = result['nextOffset']
                names = [v['name'] if isinstance(v,dict) else v for v in values]
                self.assertEqual(len(names), 1105)
                self.assertEqual(len(set(names)), 1105)
                self.assertEqual(names, sorted(names))

    def test_duplicate_columns_are_preserved_on_public_tools(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/'fixture.db'
            with closing(sqlite3.connect(path)) as connection:
                connection.execute('CREATE TABLE t(id INTEGER)')
            entry = Connection('test', URL.create('sqlite',database=str(path)))
            result = Backend(entry).query('SELECT 1 AS id, 2 AS id, 3 AS id__2')
            self.assertEqual(result['rows'], [{'id':1,'id__3':2,'id__2':3}])
            self.assertEqual(result['values'], [[1,2,3]])
            self.assertEqual(result['rowKeys'], ['id','id__3','id__2'])
            mysql = Connection('mysql', URL.create('mysql+pymysql',host='localhost'))
            with patch.object(Backend,'query',return_value=result):
                response = run_tool('db_query', {'sql':'SELECT 1'}, [mysql])
            import json
            self.assertEqual(json.loads(response.content[0].text)['rows'], result['rows'])
            self.assertEqual(response.structuredContent['rowKeys'], result['rowKeys'])
