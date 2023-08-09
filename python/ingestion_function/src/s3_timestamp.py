"""
This module contains a function for retrieving a timestamp from an S3 object.

The primary purpose of this module is to provide a utility function
that retrieves a timestamp stored in an S3 object. The function,
`get_s3_timestamp`, connects to an S3 bucket, retrieves the
specified object (filename), and returns its content as a timestamp string.

Usage:
1. Ensure the necessary library (boto3) is available.
2. Use the `get_s3_timestamp` function to retrieve a timestamp from
an S3 object.
   - Pass the filename (S3 object key) as an argument.
   - The function connects to the specified S3 bucket and retrieves
   the content of the object.
   - If the object is missing, a default timestamp ('1901-01-01 01:01:01.001')
   is returned.
   - The function raises a TypeError if the input argument is incorrect.

Example:
Assuming an S3 object named 'postgres-datetime.txt' contains a timestamp,
the following usage retrieves it: timestamp = get_s3_timestamp
("postgres-datetime.txt")
"""
import boto3
from botocore.exceptions import ClientError


def get_s3_timestamp(filename):
    """
    Retrieve a timestamp from an S3 object.

    This function connects to an AWS S3 bucket and retrieves the
    content of the specified object (filename), which is expected
    to contain a timestamp. If the object is missing in the S3 bucket,
    a default timestamp ('1901-01-01 01:01:01.001') is returned.

    Args:
        filename (str): The name (key) of the S3 object containing
        the timestamp.

    Returns:
        str: The retrieved timestamp as a string.

    Raises:
        ValueError: If the provided filename is an empty string.
        TypeError: If the provided filename is not a string.
    """
    try:
        if len(filename) == 0:
            raise ValueError("No input name")

        # connect to the s3
        s3 = boto3.client('s3')

        # get the object with the input name
        file = s3.get_object(Bucket='ingested-data-vox-indicium', Key=filename)
        timestamp = file['Body'].read()
        return timestamp.decode('utf-8')
    except ClientError as e:
        # catches the error if the user tap a non-existent table name
        # whole table uploaded in case of missing file
        if e.response['Error']['Code'] == 'NoSuchKey':
            return '1901-01-01 01:01:01.001'
    except TypeError:
        # catches the error if the user tap an incorrect input
        print(TypeError)
        raise TypeError("Function must take a string input")
