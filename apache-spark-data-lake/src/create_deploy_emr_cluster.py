"""
    This module allows to create the erm cluster and implements the deployment process in the cluster
"""
## Libraries
import configparser
from datetime import datetime
import boto3
import os
import sys
## Spark SQL Functions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
## My libraries
sys.path.insert(0, '../src')
import config as cf
import database as db

## Default
CONFIG_FILE = 'dl.cfg'

## Reading cfg file
config = configparser.ConfigParser()
config.read(CONFIG_FILE)

## Setting up Access Key and Secret Key
AWS_KEY = config.get('AWS','AWS_ACCESS_KEY_ID')
AWS_SECRET = config.get('AWS','AWS_SECRET_ACCESS_KEY')
AWS_REGION = config.get('AWS','region_name')
AWS_NAME = config.get('AWS','name')

## Setting up Environment Variables
os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS','AWS_SECRET_ACCESS_KEY')

s3_client = boto3.client('s3', region_name=AWS_REGION, aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)


def main():
    """
    Main Function:
        - Create a ERM Cluster and Deploy ETL Process
    
    Args:
        - None

    Returns:
        - None
    """   

    ## Creating IAM Client
    print('Step 1 - Creating IAM Client ' + str(datetime.now()))
    
    iam_client = boto3.client('iam')
    db.create_iam_role(iam_client, config)
    
    print('Step 1 - IAM Client Created ' + str(datetime.now()))

    ## Creating S3 Client
    print('Step 2 - Creating S3 Client ' + str(datetime.now()))
    
    s3_client = boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
    )
    
    print('Step 2 - S3 Client Created ' + str(datetime.now()))

    ## Create S3 Bucket
    print('Step 3 - Creating S3 Buckets ' + str(datetime.now()))
    
    output_bucket = config.get('S3', 'output_bucket')
    db.create_s3_bucket(s3_client, AWS_REGION, output_bucket)

    bucket_name = config.get('S3', 'source_bucket')
    db.create_s3_bucket(s3_client, AWS_REGION, bucket_name)

    print('Step 3 - Output S3 Buckets Created ' + str(datetime.now()))  
    
    
    ## Update config file
    print('Step 4 - Updating Config File ' + str(datetime.now()))
    
    s3_output_bucket = 's3a://' + output_bucket + '/'
    cf.update_cfg_file(CONFIG_FILE, 'DATALAKE', 'output_data', s3_output_bucket)
    
    print('Step 4 - Config File Updated' + str(datetime.now()))

    
    ## Setting Up S3 Buckets
    print('Step 5 - Setting Up S3 Buckets ' + str(datetime.now()))
    
    input_data = config.get('DATALAKE','input_data')
    print('Step 5.1 - Input Data: ' + input_data)
    
    output_data = config.get('DATALAKE','output_data')
    print('Step 5.1 - Output Data: ' + output_data)
    
    print('Step 5 - Set Up S3 Buckets Completed ' + str(datetime.now()))
        
    
    ## Upload Code with ERM Cluster
    print('Step 6 - Uploading Code from Local to Cluster: ' + str(datetime.now()))
    
    db.upload_code(s3_client, 'etl.py', config['S3']['source_bucket'])
    
    print('Step 6 - Code Uploaded ' + str(datetime.now()))

    ## Create a EMR Cluster
    print('Step 7 - Creating EMR Cluster: ' + str(datetime.now()))
    
    emr_client = boto3.client(
        'emr',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
    )

    db.create_emr_cluster(emr_client, config)
    
    print('Step 7 - EMR Cluster Created with Success: ' + str(datetime.now()))

    
    print('Note: Go to the AWS console - EMR service and check the cluster and job status.')

if __name__ == '__main__':
    main()