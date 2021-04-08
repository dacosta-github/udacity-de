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

# Changes Operator UI Color
PostgresOperator.ui_color = '#F98866'

## Defult Bucket Name
RAW_DATALAKE_BUCKET='complaints-raw-datalake'

## Local Path for each group of files
DATA_COMPLAINTS_s3_prefix='raw/complaints'
DATA_GEO_CITIES_s3_prefix='raw/geo/cities'
DATA_GEO_COUNTIES_s3_prefix='raw/geo/counties'
DATA_GEO_ZIP_s3_prefix='raw/geo/zip'
DATA_GEO_STATES_s3_prefix='raw/geo/states'

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

dag = DAG('dag03_run_etl_load_data_warehouse',
          default_args=default_args,
          description='Extract, Load and Transform Data from S3 to Data Warehouse in Redshift',
          schedule_interval='@daily',
          )

""" Create DAG Operators """ 

## 1º Step (Upload to S3 Operator)
load_staging_complaints_table = S3ToRedshiftOperator(
    task_id='Load_staging_complaints',
    dag=dag,
    s3_bucket=RAW_DATALAKE_BUCKET,
    s3_prefix=DATA_COMPLAINTS_s3_prefix,
    table='stg_raw_complaints',
    region='eu-west-1',
    copy_options="FORMAT as CSV DELIMITER as '|' QUOTE as '\"' IGNOREHEADER 1",
    mode='TRUNCATE'
)

load_staging_cities_table = S3ToRedshiftOperator(
    task_id='Load_staging_cities',
    dag=dag,
    s3_bucket=RAW_DATALAKE_BUCKET,
    s3_prefix=DATA_GEO_CITIES_s3_prefix,
    table='stg_raw_cities',
    region='eu-west-1',
    copy_options="FORMAT as CSV DELIMITER as ',' QUOTE as '\"' IGNOREHEADER 1",
    mode='TRUNCATE'
)

load_staging_counties_table = S3ToRedshiftOperator(
    task_id='Load_staging_counties',
    dag=dag,
    s3_bucket=RAW_DATALAKE_BUCKET,
    s3_prefix=DATA_GEO_COUNTIES_s3_prefix,
    table='stg_raw_counties',
    region='eu-west-1',
    copy_options="FORMAT as CSV DELIMITER as ',' QUOTE as '\"' IGNOREHEADER 1",
    mode='TRUNCATE'
)

load_staging_zip_table = S3ToRedshiftOperator(
    task_id='Load_staging_zip',
    dag=dag,
    s3_bucket=RAW_DATALAKE_BUCKET,
    s3_prefix=DATA_GEO_ZIP_s3_prefix,
    table='stg_raw_zip',
    region='eu-west-1',
    copy_options="FORMAT as CSV DELIMITER as ',' QUOTE as '\"' IGNOREHEADER 1",
    mode='TRUNCATE'
)

load_staging_states_table = S3ToRedshiftOperator(
    task_id='Load_staging_states',
    dag=dag,
    s3_bucket=RAW_DATALAKE_BUCKET,
    s3_prefix=DATA_GEO_STATES_s3_prefix,
    table='stg_raw_states',
    region='eu-west-1',
    copy_options="FORMAT as CSV DELIMITER as ',' QUOTE as '\"' IGNOREHEADER 1",
    mode='TRUNCATE'
)

## 2º Step (Dimension Operators)
load_dim_complaints_table = LoadDimensionOperator(
    task_id='Load_dim_complaints_table',
    dag=dag,
    table='dim_complaints',
    select_sql=SqlLoadDimensions.insert_dim_complaints_table,
    mode='DELETE'
)


load_dim_tags_table = LoadDimensionOperator(
    task_id='Load_dim_tags_table',
    dag=dag,
    table='dim_tags',
    select_sql=SqlLoadDimensions.insert_dim_tags_table,
    mode='DELETE'
)

load_dim_dates_table = LoadDimensionOperator(
    task_id='Load_dim_dates_table',
    dag=dag,
    table='dim_dates',
    select_sql=SqlLoadDimensions.insert_dim_dates_table,
    mode='DELETE'
)

load_dim_products_table = LoadDimensionOperator(
    task_id='Load_dim_products_table',
    dag=dag,
    table='dim_products',
    select_sql=SqlLoadDimensions.insert_dim_products_table,
    mode='DELETE'
)

load_dim_issues_table = LoadDimensionOperator(
    task_id='Load_dim_issues_table',
    dag=dag,
    table='dim_issues',
    select_sql=SqlLoadDimensions.insert_dim_issues_table,
    mode='DELETE'
)

load_dim_companies_table = LoadDimensionOperator(
    task_id='Load_dim_companies_table',
    dag=dag,
    table='dim_companies',
    select_sql=SqlLoadDimensions.insert_dim_companies_table,
    mode='DELETE'
)

load_dim_geographies_table = LoadDimensionOperator(
    task_id='Load_dim_geographies_table',
    dag=dag,
    table='dim_geographies',
    select_sql=SqlLoadDimensions.insert_dim_geographies_table,
    mode='DELETE'
)

## 3º Step (Fact Operators)
load_fact_complaints_table = LoadFactOperator(
    task_id='Load_fact_complaints_table',
    dag=dag,
    table='fact_complaints',
    select_sql=SqlLoadFacts.insert_fact_complaints_table,
    mode='DELETE'
)

## 4º Step (Check Operators)
data_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    operations=[
        {
            'sql': 'SELECT COUNT(*) FROM fact_complaints;',
            'operation': 'validation',
            'target': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM fact_complaints WHERE product_pk IS NULL;',
            'operation': 'error',
            'target': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM dim_products;',
            'operation': 'validation',
            'target': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM dim_complaints;',
            'operation': 'validation',
            'target': 0
        }
        ### .... include other tables
    ]
)

## 4º Step (Dummy Operators)
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
check_operator = DummyOperator(task_id='Check_execution', dag=dag)
end_operator = DummyOperator(task_id='Stop_execution', dag=dag)
staging_completed = DummyOperator(task_id='Staging_completed', dag=dag)
dimensions_completed = DummyOperator(task_id='Dimensions_completed', dag=dag)
facts_completed = DummyOperator(task_id='Facts_completed', dag=dag)


## DAG Generator with dependencies
start_operator >> [load_staging_complaints_table, load_staging_cities_table, load_staging_counties_table, load_staging_states_table, load_staging_zip_table]

[load_staging_complaints_table, load_staging_cities_table, load_staging_counties_table, load_staging_states_table, load_staging_zip_table] >> staging_completed

staging_completed >> [load_dim_companies_table,load_dim_complaints_table,load_dim_dates_table, load_dim_geographies_table, load_dim_tags_table, load_dim_products_table, load_dim_issues_table]

[load_dim_companies_table,load_dim_complaints_table,load_dim_dates_table, load_dim_geographies_table, load_dim_tags_table, load_dim_products_table, load_dim_issues_table] >> dimensions_completed

dimensions_completed >> load_fact_complaints_table

load_fact_complaints_table >> facts_completed

facts_completed >> data_quality_checks

data_quality_checks >> check_operator

check_operator >> end_operator