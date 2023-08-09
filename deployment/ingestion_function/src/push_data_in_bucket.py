import boto3
import pandas as pd
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def push_data_in_bucket(directory, file_name):

    try:

        file_path = f'{directory}{file_name}'

        log_changes_to_db(file_path, file_name)
        client = boto3.client("s3")

        client.upload_file(file_path, "ingestion-data-vox-indicium", file_name)

        print(f"The file {file_name} was uploaded")

    except FileNotFoundError as e:
        raise e
    except ClientError as e:
        raise e


def log_changes_to_db(file_path, file_name):

    try:

        file = pd.read_csv(file_path)

        num = len(file)

        logger.error(f'Number of changes made to {file_name}: {num}')

    except Exception as e:
        raise e
