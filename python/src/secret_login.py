import boto3
import json
from botocore.exceptions import ClientError


# Create a Secrets Manager client
region_name = "eu-west-2"
session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)


def retrieve_secret_details(secret_name):

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
