from pyhocon import ConfigFactory
import psycopg2
from catdb.db import Db


class Postgres(Db):
    PUBLIC_SCHEMA = 'public'

    # see http://www.postgresql.org/docs/9.3/static/datatype.html
    DATA_TYPES_MAPPING = {
        'VARCHAR': 'CHARACTER VARYING',
        'INT': 'INTEGER',
        'BIGINT': 'BIGINT',
        'TIMESTAMP': 'TIMESTAMP WITHOUT TIME ZONE',
        'JSON': 'JSON',

    }

    REV_DATA_TYPES_MAPPING = {v: k for k, v in DATA_TYPES_MAPPING.items()}

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
                    """SELECT column_name, data_type, is_nullable, character_maximum_length
                       FROM information_schema.columns
                       WHERE table_schema='{schema}' AND table_name='{table_name}'""".format(
                        schema=Postgres.PUBLIC_SCHEMA if schema is None else schema,
                        table_name=table
                    ))

                return {
                    'schema': schema,
                    'table': table,
                    'columns': [
                        {'column': column, 'data_type': Postgres.REV_DATA_TYPES_MAPPING[data_type.upper()],
                         'nullable': nullable == 'YES', 'length': length}
                        for column, data_type, nullable, length in cursor.fetchall()
                    ]
                }

    def create_table_statement(self, ddl):
        column_str = ',\n    '.join(
            entry['column'] + ' ' + entry['data_type'] for entry in ddl['columns']
        )

        return 'CREATE TABLE ' + ddl['table'] + ' (\n    ' \
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

                for row in cursor.fetchmany():
                    yield row
