"""
This script processes and loads data into a data warehouse using AWS services.

The primary purpose of this script is to process and load data
stored in Parquet files from an S3 bucket into a data warehouse
using AWS services. The script retrieves database login details
from AWS Secrets Manager, establishes a connection to the data warehouse
using psycopg2, and performs either data updates or initial data seeding
based on whether the Lambda function has been called before.

Usage:
1. Ensure the necessary libraries (psycopg2, logging, botocore) are available.
2. Import the required utility functions from 'python.loading_function.src'
and related modules.
3. Use the `push_data_in_bucket` function to process and load data into
the data warehouse.
   - This function connects to the data warehouse using retrieved credentials
   and loads Parquet data.
   - Depending on whether the Lambda function has been called before,
   it performs data updates or seeding.

Example:
Assuming AWS credentials are properly configured and AWS services are
accessible, invoking the `push_data_in_bucket` function processes and
loads data into the data warehouse.
"""
from python.loading_function.src.load_utils import (
    getDataFrameFromS3Parquet,
    list_parquet_files_in_bucket,
    has_lambda_been_called)
from python.loading_function.src.sql_utils import (
    copy_from_file,
    update_from_file,
    get_table_primary_key
)
from python.loading_function.src.secret_login import retrieve_secret_details
from botocore.exceptions import ClientError

import psycopg2
import logging

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def push_data_in_bucket(event, context):
    """
    Process and load data into a data warehouse using AWS services.

    This function connects to a data warehouse using database credentials
    retrieved from AWS Secrets Manager. It processes and loads data stored
    in Parquet files from an S3 bucket into the data warehouse. Depending
    on whether the Lambda function has been called before, the function either
    performs data updates or initializes the data warehouse with initial data.

    Args:
        event: Event data passed to the function (AWS Lambda event).
        context: Context information passed to the function
        (AWS Lambda context).

    Returns:
        None
    """
    bucket_name = "processed-data-vox-indicium"

    # Retrieve the login details from an AWS Secret Store
    try:
        db_login_deets = retrieve_secret_details("Totesys-Warehouse")
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailure':
            logger.error(
                "The requested secret can't be decrypted:", e)
        elif e.response['Error']['Code'] == 'InternalServiceError':
            logger.error('An error occurred on service side:', e)
        else:
            logger.error('Database credentials could not be retrieved:', e)
        raise e

    # Use psycopg2 and credentials from the secret store to connect
    # to our data warehouse
    try:
        connection = psycopg2.connect(
            host=db_login_deets['host'],
            port=int(db_login_deets['port']),
            database=db_login_deets['database'],
            user=db_login_deets['username'],
            password=db_login_deets['password']
        )
        logger.info('Connected to Totesys warehouse...')
    except psycopg2.Error as e:
        logger.error('Unable to connect to the warehouse:', e)
        raise e

    # If the lambda has been called before we only want to update
    # the relevant tables
    # If it has not been called before we want to seed the
    # data warehouse with initial data (easier)
    if has_lambda_been_called():
        pq_files = list_parquet_files_in_bucket()
        for file in pq_files:
            df = getDataFrameFromS3Parquet(bucket_name, file)
            table_name = file.split(".")[0]
            primary_keys_list = get_table_primary_key(connection, table_name)
            update_from_file(connection, df, table_name, primary_keys_list)
            logger.info('Updating Totesys warehouse...')
    else:
        pq_files = list_parquet_files_in_bucket()
        for file in pq_files:
            df = getDataFrameFromS3Parquet(bucket_name, file)
            table_name = file.split(".")[0]
            logger.info('Initializing Totesys warehouse:')
            copy_from_file(connection, df, table_name)

    connection.close()
