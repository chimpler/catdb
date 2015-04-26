from abc import abstractmethod
import csv
import importlib
import pkg_resources
from pyhocon import ConfigFactory


class Db(object):
    ROW_BUFFER_SIZE = 1000

    QUOTED_TYPES = [
        'JSON', 'XML', 'CHAR', 'VARCHAR', 'LONGVARCHAR', 'DATE', 'TIME', 'TIMESTAMP', 'BINARY', 'VARBINARY', 'LONGVARBINARY', 'BLOB', 'CLOB'
    ]

    def __init__(self, dbname, params={}):
        self._params = params
        self._dbname = dbname
        mappings = ConfigFactory().parse_file(pkg_resources.resource_filename(__name__, 'mappings.conf'))

        # mapping coltype => {mapping} instead of coltype => db => {mapping}
        self._mappings = {k: d[dbname] for k, d in mappings['mappings'].items()}

        # reverse mapping
        self._rev_mappings = {
            d[dbname]['type']: {
                'type': k,
                'defaults': {
                    dv: dk for dk, dv in d[dbname].get('defaults', {}).items()
                }
            } for k, d in mappings['mappings'].items()
        }

    @abstractmethod
    def list_tables(self, schema=None, filter=None):
        pass

    @abstractmethod
    def get_column_info(self, schema, table):
        pass

    @abstractmethod
    def export_data(self, schema=None, table=None):
        pass

    @abstractmethod
    def get_connection(self):
        pass

    def get_ddl(self, schema=None, table_filter=None):
        """translate db specific ddl to generic ddl"""
        def get_default(col_type, value):
            return self._rev_mappings[col_type]['defaults'].get(value)

        def get_column_def(entry):
            row = {
                'column': entry['column'],
                'radix': entry['radix'],
                'scale': entry['scale'],
                'size': entry['size'],
                'type': self._rev_mappings[entry['type']]['type'],
                'default': get_default(entry['type'], entry['default'])
            }

            # remove null values
            return {k: v for k, v in row.items() if v is not None}

        def get_table_def(table):
            meta = self.get_column_info(schema, table)
            return {
                'name': table,
                'columns': [get_column_def(col) for col in meta]
            }

        return {
            'database': self._params['database'],
            'schema': schema,
            'tables': [get_table_def(table) for table in self.list_tables(table_filter)]
        }

    def create_table_statement(self, ddl, schema, table):
        def get_default(col_type, value):
            return self._mappings[col_type]['defaults'].get(value, "'" + value + "'")

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

    def export_to_file(self, fd, schema=None, table=None):
        writer = csv.writer(fd, delimiter='|', quotechar="'", quoting=csv.QUOTE_MINIMAL)
        for row in self.export_data(schema, table):
            writer.writerow(row)

    def import_from_file(self, fd, schema=None, table=None, dry_run=False):
        conn = self.get_connection()
        reader = csv.reader(fd, delimiter='|', quotechar="'", quoting=csv.QUOTE_MINIMAL)
        # group rows in packets
        header = next(reader)

        def insert_all(rows):
            query = "INSERT INTO {schema_str}{table} ({fields})\nVALUES".format(schema_str='' if schema is None else schema + '.',
                                                                                table=table,
                                                                                fields=','.join(header)) \
                    + ',\n'.join('(' + ','.join("'" + f + "'" for f in row) + ')' for row in rows) + ';'

            if dry_run:
                print query
            else:
                conn.execute(query)

        buffer = []
        for row in reader:
            buffer.append(row)
            if len(buffer) >= Db.ROW_BUFFER_SIZE:
                insert_all(buffer)
                buffer = []

        if len(buffer) > 0:
            insert_all(buffer)


class DbManager:
    @staticmethod
    def get_db(name, params):
        module = importlib.import_module(
            'catdb.{implementation}'.format(implementation=name))
        class_name = name.capitalize()
        clazz = getattr(module, class_name)
        return clazz(params)
