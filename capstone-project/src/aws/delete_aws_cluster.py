"""
    This module provides all methods to interact with AWS Account.
    It contains the cluster and session deletion methods, iam roles, vpc, ec2, s3 and redshift.
    Note: Based on the research that was done. See in the acknowledgements the sources.
"""
import boto3
import time
import json
import configparser
import sys
sys.path.insert(0, '../config')
sys.path.insert(0, '../aws')

# Define config_file
config_file = '../config/dwh.cfg'

# Reading cfg file
config = configparser.ConfigParser()
config.read(config_file)

# Setting up Access Key and Secret Key
AWS_ACCESS_KEY_ID = config.get('AWS','AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = config.get('AWS','AWS_SECRET_ACCESS_KEY')
REGION_NAME = config.get('AWS','REGION_NAME')

# Define policy to be attached to IAM role
S3_ARN_POLICY = config.get('SECURITY','S3_ARN_POLICY')

# Define AWS Services
redshift_client = boto3.client('redshift', region_name=REGION_NAME, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
iam_client = boto3.client('iam', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
ec2_client = boto3.client('ec2', region_name=REGION_NAME, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


def delete_redshift_cluster(config):
    """
        - This function: Deletes AWS Redshift Cluster
        
        Args:
            config (ConfigParser object): Configuration File to define Resource configuration
        
        Returns:
            dictionary: AWS Redshift Information

        Returns: 
            None
    """
    try:
        response = redshift_client.delete_cluster(
            ClusterIdentifier=config.get('CLUSTER', 'CLUSTER_IDENTIFIER'),
            SkipFinalClusterSnapshot=True
        )
    except:
        print("Redshift Cluster '%s' does not exist!" % (config.get('CLUSTER', 'CLUSTER_IDENTIFIER')))
        return None
    else:
        return response['Cluster']


def wait_for_cluster_deletion(cluster_id):
    """
        - This function: Verifies if AWS Redshift Cluster was deleted
        
        Args:
            cluster_id (dictionary): AWS Redshift Cluster Information
        
        Returns: 
            None
    """
    while True:
        try:
            redshift_client.describe_clusters(ClusterIdentifier=cluster_id)
        except:
            break
        else:
            time.sleep(60)


def delete_iam_role(config, arn_policy):
    """
        - This function: Deletes AWS IAM Role
        
        Args:
            config (ConfigParser object): Configuration file to define resource configuration
            arn_policy (string): ARN Policy you want to detach from the IAM Role
        
        Returns: 
            None
    """
    try:
        iam_client.detach_role_policy(
            RoleName=config.get('SECURITY', 'ROLE_NAME'),
            PolicyArn=s3_arn_policy
        )
        iam_client.delete_role(RoleName=config.get('SECURITY', 'ROLE_NAME'))
        print('IAM Role deleted.')
    except:
        print("IAM Role '%s' doesn't exist!" % (config.get('SECURITY', 'ROLE_NAME')))


def delete_security_group(config):
    """
        - This function: Deletes AWS VPC Security Group
        
        Args:
            config (ConfigParser object): Configuration file to define resource configuration
        
        Returns: 
            None
    """
    try:
        ec2_client.delete_security_group(GroupId=config.get('SECURITY', 'SG_ID'))
        print('Security Group deleted.')
    except:
        print("Security Group '%s' doesn't exist!" % (config.get('SECURITY', 'SG_ID')))


def delete_resources():
    """
        - This function: Initiate Resources Deletion

        Args:
            None
        
        Returns: 
            None
    """
    config = configparser.ConfigParser()
    config.read(config_file)

    cluster_info = delete_redshift_cluster(config)

    if cluster_info is not None:
        print(f'Deleting Redshift cluster: {cluster_info["ClusterIdentifier"]}')
        print(f'Redshift Cluster status: {cluster_info["ClusterStatus"]}')

        print('Waiting for Redshift cluster to be deleted...')
        wait_for_cluster_deletion(cluster_info['ClusterIdentifier'])
        print('Redshift Cluster deleted.')

    delete_iam_role(config, S3_ARN_POLICY)

    delete_security_group(config)

if __name__ == "__main__":
    delete_resources()