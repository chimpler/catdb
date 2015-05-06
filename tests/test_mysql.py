import mock
from catdb.mysql import Mysql


class TestMysql(object):
    @mock.patch('catdb.mysql.pymysql')
    def test_list_tables(self, mock_pymysql):
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchall.return_value = [
            ['table_a'],
            ['table_b']
        ]
        mock_pymysql.connect.return_value.__enter__.return_value = mock_cursor

        mysql = Mysql({
            'database': 'test'
        })

        assert mysql.list_tables('test') == [
            'table_a',
            'table_b'
        ]

    @mock.patch('catdb.mysql.pymysql')
    def test_get_column_info(self, mock_pymysql):
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchall.return_value = [
            {
                'Field': 'field',
                'Type': 'VARCHAR(254)',
                'Null': 'YES',
                'Default': 'test'
            }
        ]
        mock_pymysql.connect.return_value.__enter__.return_value = mock_cursor

        mysql = Mysql({
            'database': 'test'
        })

        assert mysql.get_column_info('test', None) == [
            {
                'column': 'field',
                'type': 'varchar',
                'size': 254,
                'radix': None,
                'nullable': True,
                'default': 'test',
                'scale': None,
            }
        ]
