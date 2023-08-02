import pandas as pd
import fastparquet as fp
import boto3
import os
import io


def getDataFrameFromS3Parquet(bucket_name, file_name):
    # connect to the s3
    s3 = boto3.client('s3')
    # get the object with the input name
    file = s3.get_object(Bucket=bucket_name, Key=file_name)
    par = file['Body']
    file_like_obj = io.BytesIO(par.read())
    df = pd.read_parquet(file_like_obj, engine='fastparquet')
    return df


def getFileFromS3(bucket_name, file_name):
    # connect to the s3
    s3 = boto3.client('s3')
    # get the object with the input name
    file = s3.get_object(Bucket=bucket_name, Key=file_name)
    return file['Body'].read(), file['ResponseMetadata']['HTTPStatusCode']


def readParquetFromBytesObject(file):
    df = pd.read_parquet(io.BytesIO(file), engine='fastparquet')
    # print(df[1,1])
    return df


def list_parquet_files_in_bucket(bucket_name):
    s3 = boto3.client('s3')
    files = s3.list_objects(Bucket=bucket_name)
    return [file['Key'] for file in files['Contents'] if file['Key'].endswith('.parquet')]


def has_lambda_been_called(): 
    if os.environ.get('called_before', 'False') == 'False':
        os.environ['called_before'] = 'True'
        return False
    return True

