import boto3
import json
import logging
from botocore.exceptions import ClientError

client = boto3.client("secretsmanager")
logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

def retrieve_login_details():

    secret_name = "Totes-Login-Credentials"
    region_name = "eu-west-2"

    # Create a Secrets Manager client
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
    data = (
        "{"
        + secret
        .replace("{", "")
        .replace("}", "")
        .replace("'", '"')
        + "}"
    )
    data_info = json.loads(data)
    return data_info