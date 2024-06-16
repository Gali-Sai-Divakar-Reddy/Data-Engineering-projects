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
        'DWH_CLUSTER_TYPE': config.get('DWH', 'DWH_CLUSTER_TYPE'),
        'DWH_NODE_TYPE': config.get('DWH', 'DWH_NODE_TYPE'),
        'DWH_NUM_NODES': config.getint('DWH', 'DWH_NUM_NODES'),
        'DWH_DB_NAME': config.get('DWH', 'DWH_DB_NAME'),
        'DWH_CLUSTER_IDENTIFIER': config.get('DWH', 'DWH_CLUSTER_IDENTIFIER'),
        'DWH_MASTER_USERNAME': config.get('DWH', 'DWH_MASTER_USERNAME'),
        'DWH_MASTER_USER_PASSWORD': config.get('DWH', 'DWH_MASTER_USER_PASSWORD'),
        'DWH_IAM_ROLE_NAME': config.get('DWH', 'DWH_IAM_ROLE_NAME')
    }
    return config_values

def create_redshift_cluster(redshift_client, iam_role_arn, config_data):
    """
    Creates a Redshift cluster using the provided configuration and IAM role ARN.

    Args:
        redshift_client: Boto3 client for Redshift.
        iam_role_arn (str): The ARN of the IAM role.
        config_data (dict): Configuration data from 'config.ini'.
    
    Returns:
        dict: Details of the created Redshift cluster.
    """
    try:
        response = redshift_client.create_cluster(
            ClusterType=config_data['DWH_CLUSTER_TYPE'],
            NodeType=config_data['DWH_NODE_TYPE'],
            NumberOfNodes=config_data['DWH_NUM_NODES'],
            DBName=config_data['DWH_DB_NAME'],
            ClusterIdentifier=config_data['DWH_CLUSTER_IDENTIFIER'],
            MasterUsername=config_data['DWH_MASTER_USERNAME'],
            MasterUserPassword=config_data['DWH_MASTER_USER_PASSWORD'],
            IamRoles=[iam_role_arn]
        )
        logger.info("Creating Redshift cluster. This might take a few minutes...")
        waiter = redshift_client.get_waiter('cluster_available')
        waiter.wait(ClusterIdentifier=config_data['DWH_CLUSTER_IDENTIFIER'])
        cluster_info = redshift_client.describe_clusters(ClusterIdentifier=config_data['DWH_CLUSTER_IDENTIFIER'])['Clusters'][0]
        logger.info(f"Redshift cluster created successfully: {cluster_info}")
        return cluster_info
    except ClientError as e:
        logger.error(f"Error creating Redshift cluster: {e}")
        return None

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

    try:
        role_arn = iam_client.get_role(RoleName=config_data['DWH_IAM_ROLE_NAME'])['Role']['Arn']
    except ClientError as e:
        logger.error(f"Error retrieving IAM Role ARN: {e}")
        return

    cluster_info = create_redshift_cluster(redshift_client, role_arn, config_data)
    if cluster_info is None:
        logger.error("Failed to create Redshift cluster")
        return

if __name__ == "__main__":
    main()
