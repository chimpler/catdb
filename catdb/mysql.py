import pymysql
from catdb.db import Db


# TODO: http://stackoverflow.com/questions/4004205/mysql-show-constraints-on-tables-command
class Mysql(Db):
    PUBLIC_SCHEMA = 'public'

    def __init__(self, params):
        super(Mysql, self).__init__('mysql', params)

    def get_connection(self, is_dict_cursor=True, db=None):
        cursor_class = pymysql.cursors.DictCursor if is_dict_cursor else pymysql.cursors.SSCursor
        return pymysql.connect(host=self._params.get('hostname'),
                               user=self._params.get('username'),
                               passwd=self._params.get('password'),
                               db=self._params['database'] if db is None else db,
                               charset='utf8mb4',
                               cursorclass=cursor_class)

    def get_cursor(self, connection):
        return connection.cursor()

    def list_tables(self, table_filter=None, schema=None):
        with self.get_connection(False) as cursor:
            query = "SHOW TABLES LIKE '{filter}'".format(filter='%' if table_filter is None else table_filter)
            cursor.execute(query)
            return [table[0] for table in cursor.fetchall()]

    def get_column_info(self, table, schema=None):
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

        with self.get_connection(False) as cursor:
            cursor.execute('DESC ' + table)
            return [get_col_def(row) for row in cursor.fetchall()]
