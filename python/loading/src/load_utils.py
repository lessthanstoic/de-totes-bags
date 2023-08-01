import pandas as pd
import fastparquet as fp
import boto3


def getFileFromS3(bucket_name, file_name):
    # connect to the s3
    s3 = boto3.client('s3')
    # get the object with the input name
    file = s3.get_object(Bucket=bucket_name, Key=file_name)
    return file


def readParquet(file):
    df = pd.read_parquet(file, engine='fastparquet')
    return df


# def write

# df = readParquet("python/loading/tests/fact_sales.parquet")
# print( df.head() )

