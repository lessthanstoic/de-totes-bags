from botocore.exceptions import (EndpointConnectionError, NoCredentialsError, ClientError)
import botocore
import boto3
from moto import mock_s3
from pprint import pprint
from src.push_parquet_files import push_parquet_file
from unittest.mock import (patch, MagicMock)
import pytest

    
@mock_s3
def test_push_parquet_function():
    mock_client = boto3.client('s3')
    mock_client.create_bucket(Bucket='processed-data-vox-indicium', CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
    })
    file_path = 'parquet_files/sales_order.parquet'

    push_parquet_file(file_path)

    result = mock_client.list_objects(Bucket='processed-data-vox-indicium')['Contents'][0]['Key']

    assert result == 'sales_order.parquet'

@mock_s3
def test_push_parquet_function_wrong_path():
    mock_client = boto3.client('s3')
    mock_client.create_bucket(Bucket='processed-data-vox-indicium', CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
    })
    file_path = 'parquet_files/wrong_path.parquet'

    result = push_parquet_file(file_path)

    assert result == f"Error: File {file_path} not found."


@mock_s3
def test_push_parquet_function_endpoint_error():
    mock_client = boto3.client('s3')
    mock_client.create_bucket(Bucket='processed-data-vox-indicium', CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
    file_path = 'parquet_files/sales_order.parquet'

    # Push the error
    with patch('boto3.client') as mock_client_func:
        mock_client_func.return_value.put_object.side_effect = EndpointConnectionError(endpoint_url="endpoint_url")
        result = push_parquet_file(file_path)

    assert result == "Error: Unable to connect to the S3 endpoint"

def test_push_parquet_file_no_credentials_error():
    # Create a mock client to push the error NoCredentialsError
    mock_s3 =  boto3.client('s3')
    mock_s3.put_object = MagicMock(side_effect=botocore.exceptions.NoCredentialsError())

    # Run boto3.client to return a mock client
    with patch.object(boto3, 'client', return_value=mock_s3):
            file_path = 'parquet_files/sales_order.parquet'
            result = push_parquet_file(file_path)
            assert result == "Error: No credentials found."

def test_push_parquet_file_client_error():
    # Create a mock client with ClientError
    mock_s3 = boto3.client('s3')
    error_msg = "Error: 404"
    exception = botocore.exceptions.ClientError({"Error": {"Message": error_msg}}, "HeadObject")
    mock_s3.put_object = MagicMock(side_effect=exception)

    # Run boto3.client to return mock client
    with patch.object(boto3, 'client', return_value=mock_s3):
        file_path = 'parquet_files/sales_order.parquet'
        result = push_parquet_file(file_path)
        assert result == f"Error: {exception}"
     