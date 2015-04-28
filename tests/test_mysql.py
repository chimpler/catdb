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
