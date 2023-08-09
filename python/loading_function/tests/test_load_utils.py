from python.loading_function.src.load_utils import (
    getFileFromS3,
    readParquetFromBytesObject,
    getDataFrameFromS3Parquet,
    list_parquet_files_in_bucket,
    has_lambda_been_called)
from moto import mock_s3
import boto3
from botocore.exceptions import ClientError
from pytest import raises


@mock_s3
def create_mock_s3():
    mock_client = boto3.client('s3')
    mock_client.create_bucket(Bucket='processed-data-vox-indicium',
                              CreateBucketConfiguration={
                                  'LocationConstraint': 'eu-west-2', })
    with open('python/loading_function/tests/fact_sales.parquet', 'rb') as data:  # noqa: E501
        mock_client.put_object(Bucket='processed-data-vox-indicium',
                               Body=data,
                               Key='fact_sales.parquet')


@mock_s3
def test_can_read_file_from_s3_bucket():
    create_mock_s3()
    file, status = getFileFromS3('processed-data-vox-indicium',
                                 'fact_sales.parquet')
    assert status == 200


@mock_s3
def test_get_file_from_s3_client_error():
    create_mock_s3()
    with raises(ClientError):
        getFileFromS3('ingested-bucket', 'file.parquet')


@mock_s3
def test_can_load_parquet_to_dataframe():
    create_mock_s3()
    df = getDataFrameFromS3Parquet('processed-data-vox-indicium',
                                   'fact_sales.parquet')
    assert 1 == 1
    assert df.iloc[1].loc["1"] == 3


@mock_s3
def test_can_load_parquet_to_dataframe_two_functions():
    create_mock_s3()
    file, status = getFileFromS3('processed-data-vox-indicium',
                                 'fact_sales.parquet')
    df = readParquetFromBytesObject(file)
    assert status == 200
    assert df.iloc[1].loc["1"] == 3


@mock_s3
def test_get_data_from_s3_parquet_client_error():
    create_mock_s3()
    with raises(ClientError):
        getDataFrameFromS3Parquet('ingested-bucket', 'file.parquet')


@mock_s3
def create_mock_s3_with_multiple_objects():
    mock_client = boto3.client('s3')
    mock_client.create_bucket(Bucket='processed-data-vox-indicium',
                              CreateBucketConfiguration={
                                  'LocationConstraint': 'eu-west-2', })
    with open('python/loading_function/tests/fact_sales.parquet', 'rb') as data:  # noqa: E501
        mock_client.put_object(Bucket='processed-data-vox-indicium',
                               Body=data,
                               Key='fact_sales.parquet')
    with open('tasks.txt', 'rb') as data:
        mock_client.put_object(Bucket='processed-data-vox-indicium',
                               Body=data,
                               Key='tasks.txt')


@mock_s3
def test_lists_parquet_files_in_the_bucket():
    create_mock_s3_with_multiple_objects()
    obj = list_parquet_files_in_bucket('processed-data-vox-indicium')
    assert obj == ['fact_sales.parquet']


@mock_s3
def test_list_parquet_files_wrong_bucket_error():
    create_mock_s3_with_multiple_objects()
    with raises(ClientError):
        list_parquet_files_in_bucket("NotABucket")


@mock_s3
def test_environment_variable_returns_false_if_not_set():
    assert not has_lambda_been_called()


@mock_s3
def test_environment_variable_returns_true_if_set():
    has_lambda_been_called()
    assert has_lambda_been_called()
