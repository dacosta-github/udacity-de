"""
    This module has a set of functions that allow you to interact with AWS account, \
    such as create S3 bucket, create EMR Cluster with the 3 steps, among other administrative functions
"""
import configparser
from datetime import datetime
import boto3
import json
import os
import sys
## Spark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
## Local
sys.path.insert(0, '../src')
import config as cf

def create_s3_bucket(s3_client, bucket_location, bucket_name):
    """
    This Function:
        - Creates S3 Bucket based on AWS Region and Bucket name
    
    Args:
        - param: s3_client (object): S3 client
        - param: bucket_location (string): AWS Region to create S3 Bucket
        - param: bucket_name (string): S3 Bucket Name
    
    Returns:
        - None
    """
    
    try:
        location = {'LocationConstraint': bucket_location}
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
        print('S3 Bucket created: ', bucket_name, location)
    
    except:
        print('S3 Bucket already exists: ', bucket_name, location)


def create_spark_session():
    """
    This Function:
        - Creates an Spark Session based on the configuration required
    
    Args:
        - None

    Returns:
        - spark (object): An activate local Spark Session - hadoop-aws:2.7.0
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    return spark


def create_emr_cluster(emr_client, config):
    cluster_id = emr_client.run_job_flow(
        Name=config['EMR']['name'],
        ReleaseLabel='emr-5.28.0',
        LogUri=config['EMR']['log_uri'],
        Applications=[
            {
                'Name': 'Spark'
            },
        ],
        Configurations=[
            {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": {
                            "PYSPARK_PYTHON": "/usr/bin/python3"
                        }
                    }
                ]
            }
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 4,
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': False, ## terminate job
            'TerminationProtected': False,
        },
        Steps=[
            {
                'Name': 'Setup Debugging',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['state-pusher-script']
                }
            },
            {
                'Name': 'Setup - Copy Files',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['aws', 
                             's3', 
                             'cp', 
                             's3://' + config['S3']['source_bucket'], '/home/hadoop/',
                             '--recursive']
                }
            },
            {
                'Name': 'Run Spark',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', 
                             '/home/hadoop/etl.py',
                             config['DATALAKE']['input_data'], 
                             config['DATALAKE']['output_data']]
                }
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole=config['EMR']['job_flow_role'],
        ServiceRole=config['EMR']['service_role']
    )

    print('EMR cluster created with the JobFlowId:', cluster_id['JobFlowId'])
    return cluster_id


def create_iam_role(iam_client, config):
    """
    This Function:
        - Creates IAM Role on AWS
    
    Args:
        - param: iam_client (object): S3 iam client
    
    Returns:
        - None
    """
    
    try:
        response = iam_client.get_role(RoleName=config['EMR']['service_role'])
        print('IAM Role already exists: ' + response['Role']['Arn'])
        return response
    except:
        response = None

    if response is None:
        try:
            role = iam_client.create_role(
                RoleName=config['EMR']['service_role'],
                Description='Allows EMR to call AWS services on your behalf',
                AssumeRolePolicyDocument=json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [{
                        'Action': 'sts:AssumeRole',
                        'Effect': 'Allow',
                        'Principal': {'Service': 'elasticmapreduce.amazonaws.com'}
                    }]
                })
            )

            iam_client.attach_role_policy(
                RoleName=config['EMR']['service_role'],
                PolicyArn=config['EMR']['policy_arn_1']
            )

            iam_client.attach_role_policy(
                RoleName=config['EMR']['service_role'],
                PolicyArn=config['EMR']['policy_arn_2']
            )

            print('IAM Role Created: ', config['EMR']['service_role'])
            return role
        
        except ClientError as e:
            print(e)


def wait_for_cluster_creation(s3_client, cluster_id):
    """
    - This function: 
        - Verifies if AWS Redshift Cluster was created
    
    Args:
      param: s3_client (object): 
      param: cluster_id (string): AWS Redshift Cluster Name
    
    Returns:
      dictionary: AWS Redshift Cluster Information
    """
    
    while True:
        response = s3_client.describe_clusters(ClusterIdentifier=cluster_id)
        cluster_info = response['Clusters'][0]
        if cluster_info['ClusterStatus'] == 'available':
            break
        time.sleep(60)

    return cluster_info


def upload_code(s3_client, file_name, bucket_name):
    """
    This Function:
        - Creates S3 Bucket based on AWS Region and Bucket name
    
    Args:
        - param: cs3_client (object): S3 client
        - param: cfile_name (string): 
        - param: cbucket_name (string): S3 Bucket Name
    
    Returns:
        - None
    """

    s3_client.upload_file(file_name, bucket_name, 'etl.py')
    print('Code uploaded in S3 bucket with success:', file_name, bucket_name)