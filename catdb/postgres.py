import psycopg2
from catdb.db import Db


class Postgres(Db):
    PUBLIC_SCHEMA = 'public'

    DEFAULT_VALUE_MAPPING = {
        'CURRENT_TIMESTAMP': 'NOW()'
    }

    def __init__(self, params):
        super(Postgres, self).__init__('postgres', params)

    def __get_connect_params(self):
        return {
            'database': self._params['database'],
            'host': self._params.get('hostname'),
            'port': self._params.get('port', None),
            'user': self._params.get('username', None),
            'password': self._params.get('password', None)
        }

    def get_connection(self, use_dict_cursor=False, db=None):
        params = {}
        params.update(self.__get_connect_params())
        if use_dict_cursor:
            params.update({
                'cursor_factory': 'psycopg2.extras.RealDictCursor'
            })
        return psycopg2.connect(**params)

    def list_tables(self, schema=None, filter=None):
        with psycopg2.connect(**self.__get_connect_params()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name LIKE '{filter}'".format(
                        schema=Postgres.PUBLIC_SCHEMA if schema is None else schema,
                        filter='%' if filter is None else filter
                    ))
                return [table[0] for table in cursor.fetchall()]

    # http://stackoverflow.com/questions/2204058/list-columns-with-indexes-in-postgresql
    # http://www.alberton.info/postgresql_meta_info.html#.VT2sIhPF-d4
    def get_column_info(self, schema, table):
        with psycopg2.connect(**self.__get_connect_params()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """SELECT
                        column_name,
                        data_type,
                        column_default,
                        is_nullable,
                        character_maximum_length,
                        numeric_precision,
                        numeric_precision_radix,
                        numeric_scale
                       FROM information_schema.columns
                       WHERE table_schema='{schema}' AND table_name='{table_name}'""".format(
                        schema=Postgres.PUBLIC_SCHEMA if schema is None else schema,
                        table_name=table
                    ))

                return [
                    {
                        'column': column,
                        'type': data_type.lower(),
                        'default': None if default_value is None else default_value.lower(),
                        'nullable': nullable,
                        'size': length if length is not None else numeric_precision,
                        'radix': numeric_precision_radix,
                        'scale': numeric_scale
                    } for
                    column, data_type, default_value, nullable, length, numeric_precision, numeric_precision_radix, numeric_scale
                    in cursor.fetchall()
                ]
