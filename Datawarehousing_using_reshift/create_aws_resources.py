import boto3
import json
import logging
import configparser
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_config():
    """
    Reads the configuration settings from 'config.ini'.
    Retrieves database connection parameters.

    Returns:
        dict: A dictionary containing configuration values such as database name, host, username, etc.
    """
    # Create a ConfigParser object
    config = configparser.ConfigParser()
 
    # Read the configuration file
    config.read('config.ini')
 
    # Access values from the configuration file
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    VPC_ID = config.get('AWS', 'VPC_ID')
    NAME= config.get('SECURITY_GROUP', 'NAME')
    DESCRIPTION=config.get('SECURITY_GROUP', 'DESCRIPTION')
    PORT=config.getint('SECURITY_GROUP', 'PORT')
    IP_RANGE=config.get('SECURITY_GROUP', 'IP_RANGE')
    DWH_IAM_ROLE_NAME=config.get('DWH', 'DWH_IAM_ROLE_NAME')
 
    # Return a dictionary with the retrieved values
    config_values = {
        'KEY': KEY,
        'SECRET': SECRET,
        'VPC_ID': VPC_ID,
        'NAME': NAME,
        'DESCRIPTION':DESCRIPTION,
        'PORT': PORT,
        'IP_RANGE': IP_RANGE,
        'DWH_IAM_ROLE_NAME': DWH_IAM_ROLE_NAME
    }
 
    return config_values

def create_security_group(ec2_client, config_data):
    
    try:
        response = ec2_client.create_security_group(
            GroupName=config_data['NAME'],
            Description=config_data['DESCRIPTION'],
            VpcId=config_data['VPC_ID']
        )
        security_group_id = response['GroupId']
        logger.info(f'Created security group with ID: {security_group_id}')
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidGroup.Duplicate':
            # The security group already exists
            response = ec2_client.describe_security_groups(
                Filters=[
                    {'Name': 'group-name', 'Values': [config_data['NAME']]},
                    {'Name': 'vpc-id', 'Values': [config_data['VPC_ID']]}
                ]
            )
            security_group_id = response['SecurityGroups'][0]['GroupId']
            logger.info(f'Security group already exists. Using existing security group with ID: {security_group_id}')
        else:
            logger.error(f'Error creating security group: {e}')
            return None

    # Create inbound rule for security group 
    try:
        # Add the inbound rule to the security group
        response = ec2_client.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {
                    # Ingress open to all IP addresses on the specified port
                    "IpProtocol": "tcp",
                    "FromPort": config_data['PORT'],
                    "ToPort": config_data['PORT'],
                    "IpRanges": [{"CidrIp": config_data['IP_RANGE']}],
                }
            ]
        )
        logger.info('Inbound rule added to the security group.')
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidPermission.Duplicate':
            logger.info('Inbound rule already exists for the specified port and IP range')
        else:
            logger.error(f'Error adding the inbound rule: {e}')
    
    return security_group_id

def create_DWH_IAM_Role(iam_client, config_data):
    #create Iam role which Allows Redshift cluster to call AWS service on your behalf and give amazon s3 full access attach policy.
    try:
        logger.info('Creating a new IAM Role')
        dwh_role = iam_client.create_role(
            Path='/',
            RoleName=config_data['DWH_IAM_ROLE_NAME'],
            Description='Allows Redshift cluster to call AWS service on your behalf.',
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                            'Effect': 'Allow', 
                            'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )
        # Attach Policy
        iam_client.attach_role_policy(RoleName=config_data['DWH_IAM_ROLE_NAME'],
                                      PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess"
                                      )['ResponseMetadata']['HTTPStatusCode']
        role_arn = iam_client.get_role(RoleName=config_data['DWH_IAM_ROLE_NAME'])['Role']['Arn']
        logger.info(f'IAM Role ARN: {role_arn}')
        return role_arn
    except ClientError as e:
        logger.error(f'Error creating IAM Role: {e}')
        return None

def main():
    config_data = read_config()

    ec2_client = boto3.client('ec2',
                          region_name='us-east-2',
                          aws_access_key_id = config_data['KEY'],
                          aws_secret_access_key=config_data['SECRET'])
    
    # Create the IAM role
    iam_client = boto3.client('iam',
                       region_name='us-east-2',
                       aws_access_key_id=config_data['KEY'],
                       aws_secret_access_key=config_data['SECRET'])
    
    security_group_id = create_security_group(ec2_client, config_data)
    if security_group_id is None:
        logger.error("Failed to create or retrieve security group")
        return
    # create_DWH_IAM_Role(iam)
    role_arn = create_DWH_IAM_Role(iam_client, config_data)
    if role_arn is None:
        logger.error("Failed to create IAM Role")
        return

if __name__ == "__main__":
    main()