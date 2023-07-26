import boto3
from pprint import pprint

def push_data_in_bucket(file_path, file_name):

    try: 
        client = boto3.client("s3")

        client.upload_file(file_path, "ingested-data-vox-indicium", file_name)

        print(f"The file {file_name} was uploaded")

    except Exception as e:
        raise e