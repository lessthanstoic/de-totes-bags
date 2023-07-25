import boto3
import json
from pprint import pprint

secretsmanager = boto3.client("secretsmanager")

def create_secret(name, user_id, password):
    response = secretsmanager.create_secret(
    Name= name,
    SecretString=f'{{"username":{user_id},"password":{password}}}'
    )
    print('Secret saved.')
    return response


