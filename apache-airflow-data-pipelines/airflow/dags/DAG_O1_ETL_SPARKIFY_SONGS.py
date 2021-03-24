from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

import sys
sys.path.insert(0, '../airflow/plugins/operators')
sys.path.insert(0, '../airflow/plugins/helpers')
sys.path.insert(0, '../operators')

from operators import S3ToRedshiftOperator
from operators import LoadFactOperator
from operators import LoadDimensionOperator
from operators import DataQualityOperator

from helpers import SqlQueries

## DAG Configs:
default_args = {
    'owner': 'dacosta-github',
    'start_date': datetime.now(),
    'end_date': datetime.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': 300,
    'catchup': True,
    'email_on_retry': False
}

dag = DAG('DAG_O1_ETL_SPARKIFY_SONGS',
          default_args=default_args,
          description='Extract, Transform and Load (ETL) Sparkify Song and LogS data from S3 to Redshift with Data Quality.',
          schedule_interval='@hourly',
          max_active_runs=1
          )

""" Create DAG Operators """ 

## 1º Step (Dummy Operator)
start_operator = DummyOperator(
    task_id='Begin_execution', 
    dag=dag
)


## 2º Steps (Staging)
load_staging_events_table = S3ToRedshiftOperator(
    task_id='Load_staging_events_table',
    dag=dag,
    s3_bucket='udacity-dend',
    s3_prefix='log_data',
    table='staging_events',
    copy_options="JSON 's3://udacity-dend/log_json_path.json'"
)

load_staging_songs_table = S3ToRedshiftOperator(
    task_id='Load_staging_songs_table',
    dag=dag,
    s3_bucket='udacity-dend',
    s3_prefix='song_data',
    table='staging_songs',
    copy_options="FORMAT AS JSON 'auto'"
)


## 3º Steps (Fact table)
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    select_sql=SqlQueries.insert_songplays_table
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    table='users',
    select_sql=SqlQueries.insert_users_table,
    mode='DELETE'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag, table='songs',
    select_sql=SqlQueries.insert_songs_table,
    mode='DELETE'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    table='artists',
    select_sql=SqlQueries.insert_artists_table,
    mode='DELETE'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    select_sql=SqlQueries.insert_time_table,
    mode='DELETE'
)


## 4º Steps (Dimensions Tables)
data_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    operations=[
        {
            'sql': 'SELECT COUNT(*) FROM public.songplays;',
            'operation': 'validation',
            'target': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM public.songplays WHERE song_id IS NULL;',
            'operation': 'error',
            'target': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM public.songs;',
            'operation': 'validation',
            'target': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM public.artist;',
            'operation': 'validation',
            'target': 0
        }
        ### .... include other tables
    ]
)

## 5º Steps (Operators)
end_operator = DummyOperator(
    task_id='Stop_execution', 
    dag=dag
)


## DAG dependencies

# 1. Staging phase 
start_operator >> load_staging_events_table
start_operator >> load_staging_songs_table

# 2. Fact phase
load_staging_events_table >> load_songplays_table
load_staging_songs_table >> load_songplays_table

# 3. Dimension phase 
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

# 4. Data Quality phase
load_song_dimension_table >> data_quality_checks
load_user_dimension_table >> data_quality_checks
load_artist_dimension_table >> data_quality_checks
load_time_dimension_table >> data_quality_checks

data_quality_checks >> end_operator