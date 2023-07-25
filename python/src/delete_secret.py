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
    


def delete_secrets():
    print(list_secrets())
    secret_to_delete = input("Specify Secret to delete: ")
    secrets = client.list_secrets()
    

    for secret in secrets["SecretList"]:
        if secret_to_delete == secret["Name"]:
            secret_arn = secret["ARN"]
            response = client.delete_secret(SecretId=secret_arn)
            print("deleted")
    if "secret_arn" not in locals():
        print("This is not a secret")

    print(list_secrets())



