import pandas as pd
import fastparquet as fp
import boto3
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


def has_lambda_been_called(lambda_name):
    # cloudwatch_client = boto3.client('cloudwatch')
    # cloudwatch_client.get_metric_statistics( 
    #     Namespace='AWS/lambda',
    #     MetricName='Invocations Count',
    #     Dimensions=[
    #         {
    #             'Name': 'CalledBefore',
    #             'Value': lambda_name
    #         },
    #     ],
    #     StartTime=datetime(2015, 1, 1),
    #     EndTime=datetime(2015, 1, 1),
    #     Period=123,
    #     Statistics=[
    #         'SampleCount'|'Average'|'Sum'|'Minimum'|'Maximum',
    #     ],
    #     ExtendedStatistics=[
    #         'string',
    #     ],
    #     Unit='Seconds'|'Microseconds'|'Milliseconds'|'Bytes'|'Kilobytes'|'Megabytes'|'Gigabytes'|'Terabytes'|'Bits'|'Kilobits'|'Megabits'|'Gigabits'|'Terabits'|'Percent'|'Count'|'Bytes/Second'|'Kilobytes/Second'|'Megabytes/Second'|'Gigabytes/Second'|'Terabytes/Second'|'Bits/Second'|'Kilobits/Second'|'Megabits/Second'|'Gigabits/Second'|'Terabits/Second'|'Count/Second'|'None')
