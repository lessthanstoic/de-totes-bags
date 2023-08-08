from src.load_utils import (
    getDataFrameFromS3Parquet,
    list_parquet_files_in_bucket,
    has_lambda_been_called)
from src.sql_utils import (
    copy_from_file,
    update_from_file,
    get_table_primary_key
)
from src.secret_login import retrieve_secret_details
from botocore.exceptions import ClientError

import psycopg2
import logging

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def push_data_in_bucket(event, context):

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
        pq_files = list_parquet_files_in_bucket(bucket_name)
        for file in pq_files:
            df = getDataFrameFromS3Parquet(bucket_name, file)
            table_name = file.split(".")[0]
            primary_keys_list = get_table_primary_key(connection, table_name)
            update_from_file(connection, df, table_name, primary_keys_list)
            logger.info('Updating Totesys warehouse...')
    else:
        pq_files = list_parquet_files_in_bucket(bucket_name)
        for file in pq_files:
            df = getDataFrameFromS3Parquet(bucket_name, file)
            table_name = file.split(".")[0]
            copy_from_file(connection, df, table_name)
            logger.error('Initializing Totesys warehouse:')

    connection.close()
