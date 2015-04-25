from pyhocon import ConfigFactory
import psycopg2
from catdb.db import Db


class Postgres(Db):
    PUBLIC_SCHEMA = 'public'

    # see https://db.apache.org/ddlutils/schema/
    # see http://www.postgresql.org/docs/9.3/static/datatype.html
    # TODO WITH OR WITHOUT TIMEZONE
    # TODO ENUM
    # type, reverse mapping, opt_format, quoted
    DATA_TYPES_MAPPING = {
        'XML': ('XML', True, None),
        'JSON': ('JSON', True, None),
        'BOOLEAN': ('BOOLEAN', True, None),
        'BIT': ('BIT', True, None),
        'VARBIT': ('BIT VARYING', True, None),
        'TINYINT': ('SMALLINT', False, None),
        'SMALLINT': ('SMALLINT', True, None),
        'INTEGER': ('INTEGER', True, None),
        'BIGINT': ('BIGINT', True, None),
        'FLOAT': ('REAL', False, None),
        'DOUBLE': ('DOUBLE PRECISION', True, None),
        'REAL': ('REAL', True, None),
        'NUMERIC': ('NUMERIC', True, '{size},{scale}'),
        'DECIMAL': ('NUMERIC', False, '{size},{scale}'),
        'CHAR': ('CHARACTER', True, '{size}'),
        'VARCHAR': ('CHARACTER VARYING', True, '{size}'),
        'LONGVARCHAR': ('CHARACTER VARYING', False, '{size}'),
        'DATE': ('DATE', True, None),
        'TIME': ('TIME WITHOUT TIME ZONE', True, None),
        'TIMESTAMP': ('TIMESTAMP WITHOUT TIME ZONE', True, None),
        'BINARY': ('BYTEA', True, None),
        'VARBINARY': ('BYTEA', False, None),
        'LONGVARBINARY': ('BYTEA', False, None),
        'BLOB': ('BYTEA', False, None),
        'CLOB': ('TEXT', True, None)
    }

    DEFAULT_VALUE_MAPPING = {
        'CURRENT_TIMESTAMP': 'NOW()'
    }

    REV_DATA_TYPES_MAPPING = {d[0]: k for k, d in DATA_TYPES_MAPPING.items() if d[1]}
    REV_DEFAULT_VALUE_MAPPING = {v: k for k, v in DEFAULT_VALUE_MAPPING.items()}

    def __get_connect_params(self):
        return {
            'database': self._params['database'],
            'host': self._params.get('hostname'),
            'port': self._params.get('port'),
            'user': self._params.get('username'),
            'password': self._params.get('password')
        }

    def list_tables(self, schema=None, filter=None):
        with psycopg2.connect(**self.__get_connect_params()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name LIKE '{filter}'".format(
                        schema=Postgres.PUBLIC_SCHEMA if schema is None else schema,
                        filter='%' if filter is None else filter
                    ))
                return [table[0] for table in cursor.fetchall()]

    def describe_table(self, schema, table):
        def parse_default_value(default_Value):
            formatted_value = default_value.split(':')[0].strip("'").upper() if default_value else None
            return Postgres.REV_DEFAULT_VALUE_MAPPING.get(formatted_value, formatted_value)

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

                return {
                    'schema': schema,
                    'table': table,
                    'columns': [
                        {
                            'column': column,
                            'data_type': Postgres.REV_DATA_TYPES_MAPPING[data_type.upper()],
                            'default': parse_default_value(default_value),
                            'nullable': nullable == 'YES',
                            'size': numeric_precision if length is None else length,
                            'radix': numeric_precision_radix,
                            'scale': numeric_scale
                        }
                        for
                        column, data_type, default_value, nullable, length, numeric_precision, numeric_precision_radix, numeric_scale
                        in cursor.fetchall()
                    ]
                }

    def export_data(self, schema=None, table=None):
        with psycopg2.connect(**self.__get_connect_params()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM {schema}.{table_name}".format(
                        schema=Postgres.PUBLIC_SCHEMA if schema is None else schema,
                        table_name=table
                    ))

                first = True
                yield [desc[0] for desc in cursor.description]
                while first or rows:
                    rows = cursor.fetchmany()
                    for row in rows:
                        yield row
                    first = False

    def get_connection(self):
        conn = psycopg2.connect(self.__get_connect_params())
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
