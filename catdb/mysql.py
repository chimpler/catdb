import psycopg2
import pymysql
from catdb.db import Db


class Mysql(Db):
    PUBLIC_SCHEMA = 'public'

    # TODO 'DEFAULT CURRENT TIMESTAMP' SHOULD NOT BE QUOTED
    # type, reverse mapping, opt_format, quoted
    DATA_TYPES_MAPPING = {
        'XML': ('TEXT', False, None),
        'JSON': ('TEXT', False, None),
        'BOOLEAN': ('BOOLEAN', True, None),
        'BIT': ('BIT', True, '{size}'),
        'VARBIT': ('BIT', False, '{size}'),
        'TINYINT': ('TINYINT', True, None),
        'SMALLINT': ('SMALLINT', True, None),
        'INTEGER': ('INT', True, None),
        'BIGINT': ('BIGINT', True, None),
        'FLOAT': ('FLOAT', True, '{size},{scale}'),
        'DOUBLE': ('DOUBLE', True, '{size},{scale}'),
        'REAL': ('DOUBLE', False, '{size},{scale}'),
        'NUMERIC': ('DECIMAL', True, '{size},{scale}'),
        'DECIMAL': ('NUMERIC', False, '{size},{scale}'),
        'CHAR': ('CHARACTER', True, '{size}'),
        'VARCHAR': ('VARCHAR', True, '{size}'),
        'LONGVARCHAR': ('VARCHAR', False, '{size}'),
        'DATE': ('DATE', True, None),
        'TIME': ('TIME', True, None),
        'TIMESTAMP': ('TIMESTAMP', True, None),
        'BINARY': ('BINARY', True, '{size}'),
        'VARBINARY': ('VARBINARY', True, '{size}'),
        'LONGVARBINARY': ('VARBINARY', False, None),
        'BLOB': ('BLOB', True, None),
        'CLOB': ('TEXT', True, None)
    }

    REV_DATA_TYPES_MAPPING = {d[0]: k for k, d in DATA_TYPES_MAPPING.items() if d[1]}

    def __get_connection(self):
        return pymysql.connect(host=self._params['hostname'],
                               user=self._params['username'],
                               passwd=self._params['password'],
                               db=self._params['database'],
                               charset='utf8mb4',
                               cursorclass=pymysql.cursors.DictCursor)

    def list_tables(self, schema=None, filter=None):
        conn = self.__get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute('show tables')
                return [table.values()[0] for table in cursor.fetchall()]
        finally:
            conn.close()

    def describe_table(self, schema, table):
        def get_col_def(row):
            # DOUBLE(10,2)
            data_type_tokens = row['Type'].split('(')
            data_type = Mysql.REV_DATA_TYPES_MAPPING[data_type_tokens[0].upper()]
            size = None
            scale = None
            if len(data_type_tokens) > 1:
                sub_tokens = data_type_tokens[1].strip(')').split(',')
                size = int(sub_tokens[0])
                scale = int(sub_tokens[1]) if len(sub_tokens) > 1 else None

            return {
                'column': row['Field'],
                'data_type': data_type,
                'default': row['Default'].strip("''") if row['Default'] else None,
                'nullable': row['Null'] == 'YES',
                'size': size,
                'radix': None,
                'scale': scale
            }

        conn = self.__get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute('desc ' + table)
                return {
                    'schema': schema,
                    'table': table,
                    'columns': [get_col_def(row) for row in cursor.fetchall()]
                }
        finally:
            conn.close()

    def export_data(self, schema=None, table=None):
        conn = self.__get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute('SELECT * FROM ' + table)
                first = True
                fields = [desc[0] for desc in cursor.description]
                yield fields
                while first or rows:
                    rows = cursor.fetchmany()
                    for row in rows:
                        yield [row[f] for f in fields]
                    first = False
        finally:
            conn.close()

    def get_connection(self):

        conn = self.__get_connection()
        return Connection(conn)


class Connection(object):
    def __init__(self, conn):
        self._conn = conn

    def execute(self, query):
        with self._conn.cursor() as cursor:
            cursor.execute(query)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()
