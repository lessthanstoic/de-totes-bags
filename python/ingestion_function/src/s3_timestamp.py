import boto3
from botocore.exceptions import ClientError


def get_s3_timestamp(filename):
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
