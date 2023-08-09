"""
This module contains functions for interacting with data: reading, processing,
and uploading files to an S3 bucket, along with logging information
about the process.

Usage:
1. Ensure the necessary libraries (boto3, pandas, logging) are installed.
2. Use the push_data_in_bucket function to upload a file to an S3 bucket.
   - Provide the local directory and file name as arguments.
   - Changes to the file are logged using the log_changes_to_db function.
3. The logger records information about the upload and changes made.

Example:
Assuming a CSV file named 'data_changes.csv' is located in '/tmp/csv_files/'.
push_data_in_bucket('/tmp/csv_files/', 'data_changes.csv')
"""
import boto3
import pandas as pd
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def push_data_in_bucket(directory, file_name):
    """
    Upload a file from the local directory to an S3 bucket.

    This function uploads a specified file from the local
    directory to an S3 bucket.

    Args:
        directory (str): The local directory path where the file is located.
        file_name (str): The name of the file to be uploaded.

    Raises:
        FileNotFoundError: If the specified file is not
        found in the given directory.
        ClientError: If there's an error during the S3 upload process.

    Returns:
        None
    """
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
    """
    Log the number of changes made to a file.

    This function reads a CSV file from the specified path,
    counts the number of rows, and logs the number of
    changes made to the file.
    Args:
        file_path (str): The full path to the CSV file.
        file_name (str): The name of the file being processed.

    Raises:
        Exception: If there's an error while processing the CSV file.

    Returns:
        None
    """
    try:

        file = pd.read_csv(file_path)

        num = len(file)

        logger.info(f'Number of changes made to {file_name}: {num}')

    except Exception as e:
        raise e
