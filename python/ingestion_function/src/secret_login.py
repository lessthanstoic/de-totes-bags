"""
This module contains functions for interacting with AWS services to
retrieve secret details and manage S3 objects.

The primary purpose of this module is to provide utility functions for
working with AWS services. It includes functions to retrieve secret
details using AWS Secrets Manager and functions for interacting with
S3 objects, including retrieving timestamps from S3 objects.

Usage:
1. Ensure the necessary libraries (boto3, json, botocore) are available.
2. The `retrieve_secret_details` function connects to AWS Secrets Manager
and retrieves secret details associated with a specified secret name.
It returns a dictionary containing the secret details.
3. The `get_s3_timestamp` function retrieves a timestamp stored in an
S3 object with the specified filename. If the object is missing, a default
timestamp is returned.

Example:
Assuming AWS credentials are properly configured and AWS services are
accessible, you can use the functions to retrieve secret details and
timestamps from S3 objects.
"""
import boto3
import json
from botocore.exceptions import ClientError


def retrieve_secret_details(secret_name):
    """
    Retrieve secret details from AWS Secrets Manager.

    This function connects to AWS Secrets Manager and retrieves
    the secret details associated with the specified secret name.
    The secret details are expected to be stored as JSON-formatted data.
    The function returns a dictionary containing the retrieved secret details.

    Args:
        secret_name (str): The name of the secret to retrieve.

    Returns:
        dict: A dictionary containing the secret details.

    Raises:
        ClientError: If there's an error while accessing the secret.
    """
    # Create a Secrets Manager client
    region_name = "eu-west-2"
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    data = json.loads(secret)
    return data
