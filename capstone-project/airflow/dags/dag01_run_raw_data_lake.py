from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
import sys
sys.path.insert(0, '../airflow/plugins/operators')
sys.path.insert(0, '../airflow/plugins/helpers')
sys.path.insert(0, '../operators')
sys.path.insert(0, '../helpers')
sys.path.insert(0, '../data')

from operators import S3ToRedshiftOperator
from operators import LoadFactOperator
from operators import LoadDimensionOperator
from operators import DataQualityOperator
from operators import CreateS3BucketOperator
from operators import UploadFilesToS3Operator
from operators import CheckS3FileCount

from helpers import SqlLoadDimensions
from helpers import SqlLoadFacts
from helpers import SqlLoadStaging
from helpers import SqlDdl

RAW_DATALAKE_BUCKET='complaints-raw-datalake'

## Local Path for each group of files
RAW_DATA_COMPLAINTS_PATH='./opt/bitnami/data/raw/complaints'
RAW_DATA_GEO_ZIPS_PATH='./opt/bitnami/data/raw/geo/zip'
RAW_DATA_GEO_CITIES_PATH='./opt/bitnami/data/raw/geo/cities'
RAW_DATA_GEO_COUNTIES_PATH='./opt/bitnami/data/raw/geo/counties'
RAW_DATA_GEO_STATES_PATH='./opt/bitnami/data/raw/geo/states'

## DAG Configurations:
default_args = {
    'owner': 'dacosta-github',
    'start_date': datetime.now(),
    'end_date': datetime.now(),
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': 30,
    'catchup': True,
    'email_on_retry': False
}

dag = DAG('dag01_run_raw_data_lake',
          default_args=default_args,
          description='Load Files in Raw Data Lake',
          schedule_interval='@daily',
          max_active_runs=1
          )

""" Create DAG Operators """ 

## 1º Step (Create Bucket S3 Operator)
create_raw_datalake = CreateS3BucketOperator(
    task_id='Create_complaints_raw_datalake',
    region_name='eu-west-1',
    bucket_name=RAW_DATALAKE_BUCKET,
    dag=dag
)

## 2º Step (Upload to S3 Operator)
upload_complaints_raw_data = UploadFilesToS3Operator(
    task_id='Upload_complaints_raw_data',
    bucket_name=RAW_DATALAKE_BUCKET,
    path=RAW_DATA_COMPLAINTS_PATH,
    dag=dag
)

upload_geo_zips_raw_data = UploadFilesToS3Operator(
    task_id='Upload_geo_zips_raw_data',
    bucket_name=RAW_DATALAKE_BUCKET,
    path=RAW_DATA_GEO_ZIPS_PATH,
    dag=dag
)

upload_geo_cities_raw_data = UploadFilesToS3Operator(
    task_id='Upload_geo_cities_raw_data',
    bucket_name=RAW_DATALAKE_BUCKET,
    path=RAW_DATA_GEO_CITIES_PATH,
    dag=dag
)

upload_geo_counties_raw_data = UploadFilesToS3Operator(
    task_id='Upload_geo_counties_raw_data',
    bucket_name=RAW_DATALAKE_BUCKET,
    path=RAW_DATA_GEO_COUNTIES_PATH,
    dag=dag
)

upload_geo_states_raw_data = UploadFilesToS3Operator(
    task_id='Upload_geo_states_raw_data',
    bucket_name=RAW_DATALAKE_BUCKET,
    path=RAW_DATA_GEO_STATES_PATH,
    dag=dag
)

## 3º Step (Check Operators)
check_data_quality = CheckS3FileCount(
    task_id='Check_data_quality',
    bucket_name=RAW_DATALAKE_BUCKET,
    expected_count=25, ## FILES
    dag=dag
)

## 4º Step (End Operator)
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
created_completed = DummyOperator(task_id='Created_completed', dag=dag)

upload_completed = DummyOperator(task_id='Upload_completed', dag=dag)
check_operator = DummyOperator(task_id='Check_completed', dag=dag)
end_operator = DummyOperator(task_id='Stop_execution', dag=dag)



## DAG Generator with dependencies
start_operator >> create_raw_datalake

create_raw_datalake >> created_completed

created_completed >> [upload_complaints_raw_data, upload_geo_zips_raw_data, upload_geo_cities_raw_data,upload_geo_counties_raw_data,upload_geo_states_raw_data]

[upload_complaints_raw_data, upload_geo_zips_raw_data, upload_geo_cities_raw_data,upload_geo_counties_raw_data,upload_geo_states_raw_data] >> upload_completed

upload_completed >> check_operator

check_operator >> check_data_quality

check_data_quality >> end_operator