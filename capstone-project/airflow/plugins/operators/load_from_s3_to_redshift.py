from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

"""
    - This Operator: execute queries that allow copying the data from the source in S3 to the staging tables, in the redshift cluster
"""
class S3ToRedshiftOperator(BaseOperator):
    ui_color = '#7F625C'

    @apply_defaults
    def __init__(self,
                 s3_bucket,
                 s3_prefix,
                 table,
                 region,
                 redshift_conn_id='redshift',
                 aws_conn_id='aws_credentials',
                 copy_options='',
                 mode='',
                 *args, **kwargs):
        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.table = table
        self.region = region
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.copy_options = copy_options
        self.mode = mode

    def execute(self, context):
        aws_hook = AwsHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook("redshift")

        self.log.info(f'Deleting old data from {self.table} table...')

        if self.mode == 'TRUNCATE':
            self.log.info(f'Deleting data from {self.table} table...')
            redshift_hook.run(f'Truncate Table {self.table};')
            self.log.info("Deletion complete.")

        self.log.info(f'Preparing to stage data from {self.s3_bucket}/{self.s3_prefix} to {self.table} table...')

        copy_query = f"""
                    COPY {self.table}
                    FROM 's3://{self.s3_bucket}/{self.s3_prefix}'
                    WITH credentials 'aws_access_key_id={credentials.access_key};aws_secret_access_key={credentials.secret_key}'
                    {self.copy_options}
                    REGION as {self.region};
                """

        self.log.info('Executing COPY command...')
        redshift_hook.run(copy_query)
        self.log.info("COPY command complete.")