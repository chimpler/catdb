import pymysql
from catdb.db import Db


class Mysql(Db):
    PUBLIC_SCHEMA = 'public'

    def __init__(self, params):
        super(Mysql, self).__init__('mysql', params)

    def get_connection(self, is_dict_cursor=True):
        cursor_class = pymysql.cursors.DictCursor if is_dict_cursor else pymysql.cursors.SSCursor
        return pymysql.connect(host=self._params['hostname'],
                               user=self._params['username'],
                               passwd=self._params['password'],
                               db=self._params['database'],
                               charset='utf8mb4',
                               cursorclass=cursor_class)

    def list_tables(self, schema=None, filter=None):
        with self.get_connection(False) as cursor:
            query = "SHOW TABLES LIKE '{filter}'".format(filter='%' if filter is None else filter)
            cursor.execute(query)
            return [table[0] for table in cursor.fetchall()]

        conn = self.get_connection(False)
        try:
            with conn.cursor() as cursor:
                query = "SHOW TABLES LIKE '{filter}'".format(filter='%' if filter is None else filter)
                cursor.execute(query)
                return [table[0] for table in cursor.fetchall()]
        finally:
            conn.close()

    def get_column_info(self, schema, table):
        def get_col_def(row):
            # DOUBLE(10,2)
            data_type_tokens = row['Type'].split('(')
            data_type = data_type_tokens[0].lower()
            size = None
            scale = None
            if len(data_type_tokens) > 1:
                sub_tokens = data_type_tokens[1].strip(')').split(',')
                size = int(sub_tokens[0])
                scale = int(sub_tokens[1]) if len(sub_tokens) > 1 else None

            return {
                'column': row['Field'],
                'type': data_type,
                'default': row['Default'].strip("'").lower() if row['Default'] else None,
                'nullable': row['Null'] == 'YES',
                'size': size,
                'radix': None,
                'scale': scale
            }

        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute('DESC ' + table)
                return [get_col_def(row) for row in cursor.fetchall()]
        finally:
            conn.close()
