import boto3
import json
from pprint import pprint

client = boto3.client("secretsmanager")



def list_secrets():
    secrets_array = []
    secrets = client.list_secrets()
    number_of_secrets = len(secrets["SecretList"])
    print(f"{number_of_secrets} secret(s) available:")
    for secret in secrets["SecretList"]:
        
        secrets_array.append(secret["Name"])
    return secrets_array
    


#

