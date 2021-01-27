"""
    This module provides all methods to interact with AWS Account.
    It contains the cluster and session creation methods, iam roles, vpc, ec2, s3 and redshift.
"""

import boto3
import time
import json
import configparser
from botocore.exceptions import ClientError

# Define config_file
config_file = 'dwh.cfg'

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
redshift_client = boto3.client('redshift', 
                          region_name=REGION_NAME, 
                          aws_access_key_id=AWS_ACCESS_KEY_ID, 
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

iam_client = boto3.client('iam', 
                          aws_access_key_id=AWS_ACCESS_KEY_ID, 
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

s3 = boto3.resource('s3',
                    region_name=REGION_NAME, 
                    aws_access_key_id=AWS_ACCESS_KEY_ID, 
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

ec2_client = boto3.client('ec2', 
                          region_name=REGION_NAME, 
                          aws_access_key_id=AWS_ACCESS_KEY_ID, 
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


def update_config_file(config_file, section, key, value):
    """
    - This function: Writes to an existing config file
    
    Args:
        config_file (ConfigParser object): Configuration file the user wants to update
        section (string): The section on the config file the user wants to write
        key (string): The key the user wants to write
        value (string): The value the user wants to write
    
    Returns: 
        None
    """

    try:
        # Reading cfg file
        config = configparser.ConfigParser()
        config.read(config_file)

        #Setting  Section, Key and Value to be write on the cfg file
        config.set(section, key, value)

        # Writting to cfg file
        with open(config_file, 'w') as f:
            config.write(f)
    
    except ClientError as e:
        print(f'ERROR: {e}')


def create_iam_role(config, arn_policy):
    """
    - This function: Creates IAM Role on AWS
    
    Args:
      config (ConfigParser object): Configuration File to define Resource configuration
      arn_policy (string): ARN Policy you want to attach to the IAM Role
    
    Returns:
      dictionary: IAM Role Information
    """
    
    try:
        response = iam_client.get_role(RoleName=config.get('SECURITY', 'ROLE_NAME'))
        print('IAM Role already exists: ' + response['Role']['Arn'])
        return response
    except:
        response = None

    if response is None:
        try:
            role = iam_client.create_role(
            RoleName = config.get('SECURITY', 'ROLE_NAME'),
            Description = 'Allows Redshift to call AWS services on your behalf',
            AssumeRolePolicyDocument = json.dumps({
                'Version': '2012-10-17',
                'Statement': [{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}
                    }]
                })
            )
            iam_client.attach_role_policy(
                RoleName = config.get('SECURITY', 'ROLE_NAME'),
                PolicyArn = arn_policy
            )
            print('IAM Role Created: %s.' % (config.get('SECURITY', 'ROLE_NAME')))
            return role
        
        except ClientError as e:
          print(e)


def create_cluster_security_group():
  """
    - This function: Creates VPC Security Group on AWS
  
  Returns:
      string: Security Group ID
  """
  
  try:
    response = ec2_client.describe_security_groups(Filters= [{"Name": "group-name", "Values": [config.get('SECURITY', 'SG_NAME')]}])
  except ClientError as e:
     print(e)

  if len(response['SecurityGroups']) > 0:
    print('Security Group already exists: ' + response['SecurityGroups'][0]['GroupId'])
    return response['SecurityGroups'][0]['GroupId']
  else:
    response = None

  if response is None:
    vpc_id = config.get('SECURITY', 'VPC_ID')
    if vpc_id == "":
      response = ec2_client.describe_vpcs()
      vpc_id = response.get('Vpcs', [{}])[0].get('VpcId', '')

    try:
        response = ec2_client.create_security_group(GroupName=config.get('SECURITY', 'SG_NAME'),Description='Redshift security group',VpcId=vpc_id)
        security_group_id = response['GroupId']
        print('Security Group Created %s in vpc %s.' % (security_group_id, vpc_id))

        ec2_client.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {'IpProtocol': 'tcp',
                 'FromPort': 80,
                 'ToPort': 80,
                 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                {'IpProtocol': 'tcp',
                 'FromPort': 5439,
                 'ToPort': 5439,
                 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}
            ])
        return security_group_id
    except ClientError as e:
        print(e)


def create_redshift_cluster(config, iam_role_arn, cluster_sg_id):
  """
  - This function: Creates an Amazon Redshift cluster on AWS
  
  Args:
    config (ConfigParser object): Configuration File to define Resource configuration
    iam_role_arn (string): AWS IAM role to attached on Cluster
    cluster_sg_id (string): AWS VPC Security Group ID
  
  Returns:
    dictionary: AWS Redshift Cluster Information
  """

  try:
    response = redshift_client.describe_clusters(ClusterIdentifier=config.get('CLUSTER', 'CLUSTER_IDENTIFIER'))
    print('Redshift Cluster already exists: ' + response['Clusters'][0]['ClusterIdentifier'])
    return None
  except:
    response = None

  if response is None:
    try:
      response = redshift_client.create_cluster(
      #HW
      ClusterIdentifier=config.get('CLUSTER', 'CLUSTER_IDENTIFIER')
      ,ClusterType=config.get('CLUSTER', 'CLUSTER_TYPE')
      ,NumberOfNodes=config.getint('CLUSTER', 'NUMBER_OF_NODES')
      ,NodeType=config.get('CLUSTER', 'NODE_TYPE')
      ,PubliclyAccessible=True
      
      #Identifiers & Credentials
      ,DBName=config.get('CLUSTER', 'DB_NAME')
      ,MasterUsername=config.get('CLUSTER', 'DB_USER')
      ,MasterUserPassword=config.get('CLUSTER', 'DB_PASSWORD')
      ,Port=config.getint('CLUSTER', 'DB_PORT')
      
      #Roles (for s3 access)
      ,IamRoles=[iam_role_arn]
      ,VpcSecurityGroupIds=[cluster_sg_id]
      )
      return response['Cluster']
    except ClientError as e:
      print(f'ERROR: {e}')
      return None


def wait_for_cluster_creation(cluster_id):
    """
    - This function: Verifies if AWS Redshift Cluster was created
    
    Args:
      cluster_id (string): AWS Redshift Cluster Name
    
    Returns:
      dictionary: AWS Redshift Cluster Information
    """
    
    while True:
        response = redshift_client.describe_clusters(ClusterIdentifier=cluster_id)
        cluster_info = response['Clusters'][0]
        if cluster_info['ClusterStatus'] == 'available':
            break
        time.sleep(60)

    return cluster_info


def create_resources():
    """
    - This function: Initiate Resources Creation
    
    Args:
      None
    
    Returns:
      None
    """

    config = configparser.ConfigParser()
    config.read(config_file)

    iam_role = create_iam_role(config, S3_ARN_POLICY)
    cluster_sg_id = create_cluster_security_group()
    cluster_info = create_redshift_cluster(config, iam_role['Role']['Arn'], cluster_sg_id)

    if cluster_info is not None:
        print(f'Creating cluster: {cluster_info["ClusterIdentifier"]}')
        print(f'Cluster status: {cluster_info["ClusterStatus"]}')
        print(f'Database name: {cluster_info["DBName"]}')

        print('Waiting for cluster to be created...')
        cluster_info = wait_for_cluster_creation(cluster_info['ClusterIdentifier'])
        print(f'Cluster created.')
        print(f"Endpoint={cluster_info['Endpoint']['Address']}")

        # Writing to .cfg file
        print('Updatting CFG file...')
        update_config_file(config_file, 'CLUSTER', 'HOST', cluster_info['Endpoint']['Address'])
        update_config_file(config_file, 'SECURITY', 'ROLE_ARN', iam_role['Role']['Arn'])
        update_config_file(config_file, 'SECURITY', 'SG_ID', cluster_sg_id)
        print('CFG file Updated.')

if __name__ == "__main__":
    create_resources()