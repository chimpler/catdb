import psycopg2
import time
from catdb.postgres import Postgres
from boto.s3.connection import S3Connection


class Redshift(Postgres):

    def __init__(self, params):
        super(Redshift, self).__init__(params)

    def export_to_file(self, filename, table=None, schema=None, delimiter='|', null_value='\\N'):
        """TODO: Support explicit export to S3
        :param filename:
        :param table:
        :param schema:
        :param delimiter:
        :param null_value:
        :return:
        """
        aws_config = self._params['aws']
        key = aws_config['access_key_id']
        secret = aws_config['secret_access_key']
        bucket_name = aws_config['temp_bucket']
        prefix = aws_config['temp_prefix']
        conn = S3Connection(key, secret)
        bucket = conn.get_bucket(bucket_name)

        temp_file_prefix = 'catdb_{ts}'.format(ts=int(time.time() * 1000000))
        s3_path_prefix = 's3://{bucket}/{prefix}/{file}'.format(
            bucket=bucket_name,
            prefix=prefix,
            file=temp_file_prefix
        )
        s3_file = temp_file_prefix + '000'

        with psycopg2.connect(**self._get_connect_params()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UNLOAD ('SELECT * FROM {schema}.{table}')
                    TO '{filename}'
                    CREDENTIALS 'aws_access_key_id={aws_key};aws_secret_access_key={aws_secret}'
                    PARALLEL OFF
                    """.format(schema=schema, table=table, filename=s3_path_prefix, aws_key=key, aws_secret=secret))

        key = bucket.get_key('{prefix}/{file}'.format(prefix=prefix, file=s3_file))
        key.get_contents_to_filename(filename)
        key.delete()
