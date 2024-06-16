import boto3
import logging
import configparser
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_config():
    """
    Reads the configuration settings from 'config.ini'.
    Retrieves AWS credentials and Redshift cluster parameters.

    Returns:
        dict: A dictionary containing configuration values.
    """
    config = configparser.ConfigParser()
    config.read('config.ini')

    config_values = {
        'KEY': config.get('AWS', 'KEY'),
        'SECRET': config.get('AWS', 'SECRET'),
        'REGION': 'us-east-2',
        'DWH_CLUSTER_IDENTIFIER': config.get('DWH', 'DWH_CLUSTER_IDENTIFIER'),
        'DWH_IAM_ROLE_NAME': config.get('DWH', 'DWH_IAM_ROLE_NAME'),
        'SECURITY_GROUP_NAME': config.get('SECURITY_GROUP', 'NAME'),
        'VPC_ID': config.get('AWS', 'VPC_ID')
    }
    return config_values

def delete_redshift_cluster(redshift_client, config_data):
    """
    Deletes the Redshift cluster using the provided configuration.

    Args:
        redshift_client: Boto3 client for Redshift.
        config_data (dict): Configuration data from 'config.ini'.
    
    Returns:
        bool: True if the cluster was deleted successfully, False otherwise.
    """
    try:
        redshift_client.delete_cluster(
            ClusterIdentifier=config_data['DWH_CLUSTER_IDENTIFIER'],
            SkipFinalClusterSnapshot=True
        )
        logger.info("Deleting Redshift cluster. This might take a few minutes...")
        waiter = redshift_client.get_waiter('cluster_deleted')
        waiter.wait(ClusterIdentifier=config_data['DWH_CLUSTER_IDENTIFIER'])
        logger.info("Redshift cluster deleted successfully.")
        return True
    except ClientError as e:
        logger.error(f"Error deleting Redshift cluster: {e}")
        return False

def detach_iam_role_policy(iam_client, config_data):
    """
    Detaches the AmazonS3FullAccess policy from the IAM role.

    Args:
        iam_client: Boto3 client for IAM.
        config_data (dict): Configuration data from 'config.ini'.
    
    Returns:
        bool: True if the policy was detached successfully, False otherwise.
    """
    try:
        iam_client.detach_role_policy(
            RoleName=config_data['DWH_IAM_ROLE_NAME'],
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess"
        )
        logger.info("Detached AmazonS3FullAccess policy from IAM role.")
        return True
    except ClientError as e:
        logger.error(f"Error detaching policy from IAM role: {e}")
        return False

def delete_security_group(ec2_client, config_data):
    """
    Deletes the security group created for the Redshift cluster.

    Args:
        ec2_client: Boto3 client for EC2.
        config_data (dict): Configuration data from 'config.ini'.
    
    Returns:
        bool: True if the security group was deleted successfully, False otherwise.
    """
    try:
        response = ec2_client.describe_security_groups(
            Filters=[
                {'Name': 'group-name', 'Values': [config_data['SECURITY_GROUP_NAME']]},
                {'Name': 'vpc-id', 'Values': [config_data['VPC_ID']]}
            ]
        )
        security_group_id = response['SecurityGroups'][0]['GroupId']
        ec2_client.delete_security_group(GroupId=security_group_id)
        logger.info(f"Deleted security group with ID: {security_group_id}")
        return True
    except ClientError as e:
        logger.error(f"Error deleting security group: {e}")
        return False

def main():
    config_data = read_config()

    redshift_client = boto3.client('redshift',
                                   region_name=config_data['REGION'],
                                   aws_access_key_id=config_data['KEY'],
                                   aws_secret_access_key=config_data['SECRET'])

    iam_client = boto3.client('iam',
                              region_name=config_data['REGION'],
                              aws_access_key_id=config_data['KEY'],
                              aws_secret_access_key=config_data['SECRET'])

    ec2_client = boto3.client('ec2',
                              region_name=config_data['REGION'],
                              aws_access_key_id=config_data['KEY'],
                              aws_secret_access_key=config_data['SECRET'])

    if delete_redshift_cluster(redshift_client, config_data):
        detach_iam_role_policy(iam_client, config_data)
        delete_security_group(ec2_client, config_data)

if __name__ == "__main__":
    main()
