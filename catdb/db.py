from abc import abstractmethod
import csv
import importlib


class Db(object):
    ROW_BUFFER_SIZE = 1000

    QUOTED_TYPES = [
        'JSON', 'CHAR', 'VARCHAR', 'LONGVARCHAR', 'DATE', 'TIME', 'TIMESTAMP', 'BINARY', 'VARBINARY', 'LONGVARBINARY', 'BLOB', 'CLOB'
    ]

    def __init__(self, params={}):
        self._params = params

    @abstractmethod
    def list_tables(self, schema=None, filter=None):
        pass

    @abstractmethod
    def describe_table(self):
        pass

    @abstractmethod
    def create_table_statement(self, ddl, schema, table):
        pass

    @abstractmethod
    def export_data(self, schema=None, table=None):
        pass

    @abstractmethod
    def get_connection(self):
        pass

    def execute(self, query):
        conn = self.get_connection()
        try:
            conn.execute(query)
        except Exception as e:
            conn.rollback()
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
