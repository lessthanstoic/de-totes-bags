from python.loading.src.load_utils import ( 
    getFileFromS3,
    readParquetFromBytesObject,
    getDataFrameFromS3Parquet )
from moto import mock_s3, mock_logs
import boto3
from pytest import raises
import pandas as pd


@mock_s3
def create_mock_s3():
    mock_client = boto3.client('s3')
    mock_client.create_bucket(Bucket='processed-data-vox-indicium',
                              CreateBucketConfiguration={
                                  'LocationConstraint': 'eu-west-2', })
    with open('python/loading/tests/fact_sales.parquet', 'rb') as data:
        mock_client.put_object(Bucket='processed-data-vox-indicium',
                               Body=data,
                               Key='fact_sales.parquet')


@mock_s3
def test_can_read_file_from_s3_bucket():
    create_mock_s3()
    file, status = getFileFromS3('processed-data-vox-indicium', 'fact_sales.parquet')
    assert status == 200


@mock_s3
def test_can_load_parquet_to_dataframe():
    create_mock_s3()
    df = getDataFrameFromS3Parquet('processed-data-vox-indicium', 'fact_sales.parquet')
    assert 1 == 1
    assert df.iloc[1].loc["1"] == 3


@mock_s3
def test_can_load_parquet_to_dataframe():
    create_mock_s3()
    file, status = getFileFromS3('processed-data-vox-indicium', 'fact_sales.parquet')
    df = readParquetFromBytesObject(file)
    assert status == 200
    assert df.iloc[1].loc["1"] == 3