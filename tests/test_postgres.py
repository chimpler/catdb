import mock
from catdb.postgres import Postgres


class TestPosgres(object):

    @mock.patch('catdb.postgres.psycopg2')
    def test_list_tables(self, mock_psycopg2):
        mock_cursor = mock.MagicMock()
        mock_cursor.cursor.return_value.__enter__.return_value.fetchall.return_value = [
            ['table_a'],
            ['table_b']
        ]
        mock_psycopg2.connect.return_value.__enter__.return_value = mock_cursor

        postgres = Postgres({
            'database': 'test'
        })

        assert postgres.list_tables('test') == [
            'table_a',
            'table_b'
        ]
