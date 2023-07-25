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
    






def retrieve_secrets():
    print(list_secrets())
    secret_to_retrieve = input("Specify Secret to retrieve: ")
    secrets = client.list_secrets()
    for secret in secrets["SecretList"]:
        if secret_to_retrieve == secret["Name"]:
            secret_arn = secret["ARN"]
            response = client.get_secret_value(SecretId=secret_arn)
            with open("secrets.txt", "a") as f:
                f.write(f"{response['SecretString']}\n")
            print("Secrets stored in local file secrets.txt")
            break
    if "secret_arn" not in locals():
        print("This is not a secret")
