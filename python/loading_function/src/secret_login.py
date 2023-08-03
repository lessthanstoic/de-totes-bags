"""
Example Usage:

# Retrieve secret details using the secret name or ARN
secret_name = "my-secret-name"
# Replace this with the actual secret name or ARN
secret_details = retrieve_secret_details(secret_name)

# Now you can access the secret data using the dictionary keys
username = secret_details['username']
password = secret_details['password']
# ... and so on
"""
import boto3
import json
from botocore.exceptions import ClientError


def retrieve_secret_details(secret_name):
    """
    Retrieve the secret details from AWS Secrets Manager.

    Parameters:
        secret_name (str):
            The name or ARN of the secret to retrieve.

    Returns:
        dict: A dictionary containing the secret
            data retrieved from AWS Secrets Manager.

    Raises:
        botocore.exceptions.ClientError:
            If there is an error while retrieving the secret.
    """
    # Create a Secrets Manager client
    region_name = "eu-west-2"
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        # Retrieve the secret value using the specified secret_name
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # If there is an error while retrieving the secret, raise the exception
        raise e

    # Decrypts secret using the associated KMS key
    secret = get_secret_value_response['SecretString']

    # Convert the secret string (JSON) to a dictionary
    data = json.loads(secret)
    return data
