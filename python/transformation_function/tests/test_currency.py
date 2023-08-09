import pytest
import pandas as pd
import boto3
from moto import mock_s3
from python.transformation_function.src.dim_currency import (
    dim_currency_data_frame)


# Set up the mock S3 environment and create a CSV for testing
@pytest.fixture
def create_mock_s3():
    with mock_s3():
        s3 = boto3.client('s3', region_name='eu-west-2')
        s3.create_bucket(
            Bucket='ingested-data-vox-indicium',
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        s3.create_bucket(
            Bucket='processed-data-vox-indicium',
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        csv_data = "currency_id,currency_code,created_at,last_updated\n2,USD,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000\n"  # noqa: E501
        s3.put_object(
            Bucket='ingested-data-vox-indicium',
            Key='currency.csv', Body=csv_data)
        yield


def test_dim_currency_data_frame(create_mock_s3):
    result = dim_currency_data_frame('currency')
    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] == 1


def test_dim_currency_data_frame_invalid_table_name_type_error():
    with pytest.raises(TypeError):
        dim_currency_data_frame(123)


def test_dim_currency_data_frame_file_not_found_error(create_mock_s3):
    with pytest.raises(ValueError):
        dim_currency_data_frame('non_existent_file')


def test_dim_currency_data_frame_empty_table_name_error():
    with pytest.raises(ValueError):
        dim_currency_data_frame('')
