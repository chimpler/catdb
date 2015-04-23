from abc import abstractmethod
import importlib


class Db(object):
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
    def create_table_statement(self, ddl):
        pass

    @abstractmethod
    def export_data(self, schema=None, table=None):
        pass

    def export_to_file(self, fd, schema=None, table=None, format='csv'):
        for row in self.export_data(schema, table):
            fd.write(','.join(str(col) for col in row) + '\n')


class DbManager:
    @staticmethod
    def get_db(name, params):
        module = importlib.import_module(
            'catdb.{implementation}'.format(implementation=name))
        class_name = name.capitalize()
        clazz = getattr(module, class_name)
        return clazz(params)
