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
sys.path.insert(0, '../helpers')

from operators import S3ToRedshiftOperator
from operators import LoadFactOperator
from operators import LoadDimensionOperator
from operators import DataQualityOperator

from helpers import SqlLoadDimensions
from helpers import SqlLoadFacts
from helpers import SqlLoadStaging
from helpers import SqlDdl

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

dag = DAG('dag02_run_etl_data_lake',
          default_args=default_args,
          description='Data Lake loading from Raw to S3 in Parquet and partitioned by time vs DW tables.',
          schedule_interval='@daily',
          max_active_runs=1
          )

""" Create DAG Operators """ 

## 1º Step (Dummy Operator)
start_operator = DummyOperator(
    task_id='Begin_execution', 
    dag=dag
)



## 5º Steps (Operators)
end_operator = DummyOperator(
    task_id='Stop_execution', 
    dag=dag
)


## DAG dependencies
## TODO


# 1. Staging phase 
start_operator >> end_operator