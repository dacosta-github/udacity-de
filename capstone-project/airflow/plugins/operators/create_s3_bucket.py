from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook
from boto.exception import S3CreateError

"""
    - This Operator: Create a S3 Bucket if not exists in S3 
"""
class CreateS3BucketOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 region_name,
                 aws_conn_id='aws_credentials',
                 bucket_name='default',
                 *args, **kwargs):
        super(CreateS3BucketOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.region_name = region_name

    def execute(self, context):
        s3_hook = S3Hook(self.aws_conn_id)

        try:
            print(s3_hook.get_bucket(bucket_name=self.bucket_name))
            bucket = s3_hook.create_bucket(bucket_name=self.bucket_name, region_name=self.region_name)
        
        except S3CreateError:
            self.log.info(f'Bucket name {self.bucket_name} already exist in {self.region_name} region.')

        #s3_hook.create_bucket(bucket_name=self.bucket_name, region_name=self.region_name)

        self.log.info(f'Created {self.bucket_name} bucket in {self.region_name} region.')