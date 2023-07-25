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




def list_secrets():
    secrets = secretsmanager.list_secrets()
    number_of_secrets = len(secrets["SecretList"])
    print(f"{number_of_secrets} secret(s) available:")
    for secret in secrets["SecretList"]:
        print(secret["Name"])



def retrieve_secrets():
    secret_to_retrieve = input("Specify Secret to retrieve: ")
    secrets = secretsmanager.list_secrets()
    for secret in secrets["SecretList"]:
        if secret_to_retrieve == secret["Name"]:
            secret_arn = secret["ARN"]
            response = secretsmanager.get_secret_value(SecretId=secret_arn)
            with open("secrets.txt", "a") as f:
                f.write(f"{response['SecretString']}\n")
            print("Secrets stored in local file secrets.txt")
            break
    if "secret_arn" not in locals():
        print("This is not a secret")


print(retrieve_secrets())

#print(create_secret("Test1","Test11","Test111"))