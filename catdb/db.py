from abc import abstractmethod
import csv
import importlib
import pkg_resources
from pyhocon import ConfigFactory
from catdb import CatDbException


class Db(object):
    ROW_BUFFER_SIZE = 1000

    def __init__(self, dbname, params={}):
        self._params = params
        self._dbname = dbname
        mappings = ConfigFactory().parse_file(pkg_resources.resource_filename(__name__, 'mappings.conf'))

        # mapping coltype => {mapping} instead of coltype => db => {mapping}
        # dict() list comprehension compatible with Python 2.6 (does not support {k:v for ...})
        self._mappings = dict((k, d[dbname]) for k, d in mappings['mappings'].items())

        # reverse mapping
        self._rev_mappings = dict((d[dbname]['type'], {
                'type': k,
                'defaults': dict((dv, dk) for dk, dv in d[dbname].get('defaults', {}).items())
            }) for k, d in mappings['mappings'].items()
        )

    @abstractmethod
    def list_tables(self, table_filter=None, schema=None):
        pass

    @abstractmethod
    def get_column_info(self, table, schema=None):
        pass

    @abstractmethod
    def get_connection(self, use_dict_cursor=True):
        pass

    @abstractmethod
    def get_cursor(self, connection):
        pass

    def get_ddl(self, schema=None, table_filter=None):
        """translate db specific ddl to generic ddl"""
        def get_default(col_type, value):
            return self._rev_mappings[col_type]['defaults'].get(value, value)

        def get_rev_mapping(col_type):
            rev_mapping = self._rev_mappings.get(col_type)
            if rev_mapping is None:
                raise CatDbException('Cannot find reverse mapping {col_type}. '
                                     'Please submit an issue at https://github.com/chimpler/catdb/issues.'.format(col_type=col_type))
            else:
                return rev_mapping

        def get_column_def(entry):
            row = {
                'column': entry['column'],
                'nullable': entry['nullable'],
                'radix': entry['radix'],
                'scale': entry['scale'],
                'size': entry['size'],
                'type': get_rev_mapping(entry['type'])['type'],
                'default': get_default(entry['type'], entry['default'])
            }

            # remove null values
            return dict((k, v) for k, v in row.items() if v is not None)

        def get_table_def(table):
            meta = self.get_column_info(table, schema)
            return {
                'name': table,
                'columns': [get_column_def(col) for col in meta]
            }

        return {
            'database': self._params['database'],
            'schema': schema,
            'tables': [get_table_def(table) for table in self.list_tables(schema, table_filter)]
        }

    def create_table_statement(self, ddl, schema, table):
        def get_default(col_type, value):
            defaults = self._mappings[col_type].get('defaults', None)
            return "'%s'" % value if defaults is None else defaults.get(value, "'%s'" % value)

        def format_args(args, entry):
            return ','.join(str(entry[arg]) for arg in args if entry.get(arg))

        def column_def(entry):
            col_entry = self._mappings[entry['type']]
            col_type = col_entry['type']
            args = col_entry.get('args', None)
            type_option = '' if args is None else '(' + format_args(args, entry) + ')'
            null_str = '' if ['nullable'] else ' NOT NULL'
            default_option = '' if entry.get('default') is None else ' DEFAULT ' + get_default(entry['type'], entry['default'])
            return entry['column'] + ' ' \
                + col_type \
                + type_option \
                + null_str \
                + default_option

        column_str = ',\n    '.join(column_def(entry) for entry in ddl['columns'])
        schema_str = ('' if schema is None else schema + '.')

        return 'CREATE TABLE ' + schema_str + table + ' (\n    ' \
               + column_str \
               + '\n);'

    def create_database_statement(self, ddl, database, schema):
        return '\n'.join(self.create_table_statement(table, schema, table['name']) for table in ddl['tables'])

    def execute(self, query):
        conn = self.get_connection()
        try:
            conn.execute(query)
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()

        conn.close()

    def import_from_file(self, fd, table, schema=None, delimiter='|', null_values='\\N'):
        """default implementation that should be overriden"""
        conn = self.get_connection(False)
        reader = csv.reader(fd, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
        # group rows in packets
        header = next(reader)

        def insert_all(rows):
            query = "INSERT INTO {schema_str}{table} ({fields})\nVALUES".format(schema_str='' if schema is None else schema + '.',
                                                                                table=table,
                                                                                fields=','.join(header)) \
                    + ',\n'.join('(' + ','.join(("'" + f + "'") if f != '\\N' else 'NULL' for f in row) + ')' for row in rows) + ';'
            cursor = self.get_cursor(conn)
            cursor.execute(query)
            conn.commit()

        buffer = []
        for row in reader:
            buffer.append(row)
            if len(buffer) >= Db.ROW_BUFFER_SIZE:
                insert_all(buffer)
                buffer = []

        if len(buffer) > 0:
            insert_all(buffer)

    def export_to_file(self, fd, table=None, schema=None, delimiter='|', null_value='\\N'):
        """default implementation that should be overriden"""
        def format_row(row):
            return [null_value if e is None else e for e in row]

        writer = csv.writer(fd, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
        conn = self.get_connection(False)
        try:
            cursor = self.get_cursor(conn)
            actual_table = ('' if schema is None else schema + '.') + table
            cursor.execute('SELECT * FROM ' + actual_table)

            rows = cursor.fetchmany()
            if rows:
                # write header
                fields = [desc[0] for desc in cursor.description]
                writer.writerow(fields)
                while rows:
                    formatted_rows = [format_row(row) for row in rows]
                    writer.writerows(formatted_rows)
                    rows = cursor.fetchmany()
        finally:
            conn.close()


class DbManager:
    @staticmethod
    def get_db(name, params):
        module = importlib.import_module('catdb.{implementation}'.format(implementation=name))
        class_name = name.capitalize()
        clazz = getattr(module, class_name)
        return clazz(params)
