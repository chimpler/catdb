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
        'JSON': ('JSON', True, None),
        'BOOLEAN': ('BOOLEAN', True, None),
        'BIT': ('BIT', True, None),
        'VARBIT': ('BIT VARYING', True, None),
        'TINYINT': ('SMALLINT', False, None),
        'SMALLINT': ('SMALLINT', True, None),
        'INTEGER': ('INTEGER', True, None),
        'BIGINT': ('BIGINT', True, None),
        'FLOAT': ('REAL', False, None),
        'DOUBLE': ('DOUBLE PRECISION', True),
        'REAL': ('REAL', True),
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

    REV_DATA_TYPES_MAPPING = {d[0]: k for k, d in DATA_TYPES_MAPPING.items() if d[1]}

    def list_tables(self, schema=None, filter=None):
        with psycopg2.connect("dbname={dbname} user={user}".format(dbname=self._params['database'],
                                                                   user=self._params['username'])) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name LIKE '{filter}'".format(
                        schema=Postgres.PUBLIC_SCHEMA if schema is None else schema,
                        filter='%' if filter is None else filter
                    ))
                return [table[0] for table in cursor.fetchall()]

    def describe_table(self, schema, table):
        with psycopg2.connect("dbname={dbname} user={user}".format(dbname=self._params['database'],
                                                                   user=self._params['username'])) as conn:
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
                            'default': default_value.split(':')[0].strip("'") if default_value else None,
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

    def create_table_statement(self, ddl):
        def column_def(entry):
            col_type, _, opt_format = Postgres.DATA_TYPES_MAPPING[entry['data_type']]
            type_option = '' if opt_format is None else '(' + opt_format.format(size=entry['size'],
                                                                                scale=entry['scale']) + ')'
            default_option = '' if entry['default'] is None else ' DEFAULT ' + (
                "'" + entry['default'] + "'" if entry['data_type'] in Db.QUOTED_TYPES else entry['default'])
            return entry['column'] + ' ' + col_type + type_option + default_option

        column_str = ',\n    '.join(column_def(entry) for entry in ddl['columns'])
        schema_str = ('' if ddl['schema'] is None else ddl['schema'] + '.')
        return 'CREATE TABLE ' + schema_str + ddl['table'] + ' (\n    ' \
               + column_str \
               + '\n);'

    def export_data(self, schema=None, table=None):
        with psycopg2.connect("dbname={dbname} user={user}".format(dbname=self._params['database'],
                                                                   user=self._params['username'])) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM {schema}.{table_name}".format(
                        schema=Postgres.PUBLIC_SCHEMA if schema is None else schema,
                        table_name=table
                    ))

                first = True
                while first or rows:
                    rows = cursor.fetchmany()
                    for row in rows:
                        yield row
                    first = False

