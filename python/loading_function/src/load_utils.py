"""
A collection of utility functions for working with data stored in AWS S3
and handling Parquet files.

This module provides functions to interact with an AWS S3 bucket
and manipulate Parquet files. It utilizes the 'pandas' library
to read Parquet data into DataFrames and the 'boto3' library
for communication with AWS S3.

Functions:
    - getDataFrameFromS3Parquet(bucket_name, file_name):
        Retrieves a pandas DataFrame from an S3 bucket
        for a given Parquet file.

    - getFileFromS3(bucket_name, file_name):
        Retrieves the contents of a file stored in an S3 bucket.

    - readParquetFromBytesObject(file):
        Reads a Parquet file from a bytes-like object and returns
        it as a DataFrame.

    - list_parquet_files_in_bucket(bucket_name):
        Lists all Parquet files present in an S3 bucket.

    - has_lambda_been_called():
        Checks if a Lambda function has been called before.

These functions provide convenient ways to interact with S3 storage and
process Parquet files, making it easier to work with data in the AWS
cloud environment. They can be used in various data processing and
analysis tasks within Python applications.

Note:
    - The functions in this module require the 'pandas' and
    'boto3' libraries to be installed.
    - The AWS credentials and permissions associated with the application
    running this code must have the necessary access to
    the specified S3 bucket.
"""
import pandas as pd
import boto3
import io
import os
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def getDataFrameFromS3Parquet(bucket_name, file_name):
    """
    Retrieves a pandas DataFrame from an S3 bucket for a given Parquet file.

    Parameters:
        bucket_name (str): The name of the S3 bucket
        where the Parquet file is located.
        file_name (str): The name of the Parquet file
        in the S3 bucket.

    Returns:
        pandas.DataFrame: The DataFrame containing the data
        from the Parquet file.
    """
    try:
        # Connect to the s3
        s3 = boto3.client('s3')
        # Get the object with the input file_name from the S3 bucket
        file = s3.get_object(Bucket=bucket_name, Key=file_name)
        par = file['Body']
        file_like_obj = io.BytesIO(par.read())
        # Read the Parquet data into a DataFrame using the 'fastparquet' engine
        df = pd.read_parquet(file_like_obj, engine='fastparquet')
        return df

    except ClientError as e:
        logger.error("Client error", e)


def getFileFromS3(bucket_name, file_name):
    """
    Retrieves the contents of a file stored in an S3 bucket.

    Parameters:
        bucket_name (str): The name of the S3 bucket where the file is located.
        file_name (str): The name of the file in the S3 bucket.

    Returns:
        tuple: A tuple containing the file contents (bytes) and the
        HTTP status code of the response.
    """
    try:
        # Connect to the S3 service
        s3 = boto3.client('s3')
        # Get the object with the input file_name from the S3 bucket
        file = s3.get_object(Bucket=bucket_name, Key=file_name)
        # Return the file contents (bytes) and the HTTP status
        # code of the response
        return file['Body'].read(), file['ResponseMetadata']['HTTPStatusCode']
    except ClientError as e:
        logger.error("Client error", e)
    except TypeError:
        raise TypeError("Function must take a string input")


def readParquetFromBytesObject(file):
    """
    Reads a Parquet file from a bytes-like object and
    returns it as a DataFrame.

    Parameters:
        file (bytes): The Parquet file contents as a bytes-like object.

    Returns:
        pandas.DataFrame: The DataFrame containing
        the data from the Parquet file.
    """
    # Read the Parquet data into a DataFrame using the 'fastparquet' engine
    df = pd.read_parquet(io.BytesIO(file), engine='fastparquet')
    return df


def list_parquet_files_in_bucket(bucket_name):
    """
    Lists all Parquet files present in an S3 bucket.

    Parameters:
        bucket_name (str): The name of the S3 bucket.

    Returns:
        list: A list of Parquet file names present in the specified S3 bucket.
    """
    try:

        logging.info("Trying to list objects")
        # Connect to the S3 service
        s3 = boto3.client('s3')

        logging.info("connected to boto s3")
        # List objects in the bucket
        files = s3.list_objects(Bucket=bucket_name)
        logger.info("listed and declared to variable")
        logger.info(files)

        # Extract the keys (file names) of objects that end with '.parquet'
        return [file['Key'] for file in files['Contents']
                if file['Key'].endswith('.parquet')]
    except ClientError as e:
        logger.error("Client error", e)
    except TypeError as te:
        logger.info('''Error: Bucket does not exist - gives TypeError''')
        raise TypeError('''Function must take a string input
                         and an existing bucket''')


def has_lambda_been_called():
    """
    Checks if a Lambda function has been called before.

    This function uses an environment variable called
    'called_before' to keep track of whether
    the Lambda function has been called previously.
    If it's the first time being called, it
    sets the environment variable to 'True' and returns False.
    On subsequent calls, it returns True.

    Returns:
        bool: True if the Lambda function has been called before,
        False otherwise.
    """
    # Check if the 'called_before' environment variable exists and its value
    if os.environ.get('called_before', 'False') == 'False':
        # If not called before, set the environment variable
        # to 'True' and return False
        os.environ['called_before'] = 'True'
        return False
    # If called before, return True
    return True
