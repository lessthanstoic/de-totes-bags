import pytest
import pandas as pd
import boto3
from moto import mock_s3
from python.transformation_function.src.dim_address import (
    dim_address_data_frame,)


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
        # csv_data = "1,EUR,2022-11-03 14:20:51.563000,
        # 2022-11-03 14:20:51.563000\n"
        csv_data = "address_id,address_line_1,address_line_2,district,city,postal_code,country,phone,created_at,last_updated\n1,93 Hospital Hill,Eastbourne,Frome,Egburth,M4 4DE,United Kingdom,0151 223 4352,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000"  # noqa: E501
        s3.put_object(
            Bucket='ingested-data-vox-indicium',
            Key='address.csv', Body=csv_data)
        yield


def test_dim_address_data_frame(create_mock_s3):
    result = dim_address_data_frame('address')
    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] == 1


def test_dim_address_data_frame_invalid_table_name_type_error():
    with pytest.raises(TypeError):
        dim_address_data_frame(123)


def test_dim_address_data_frame_file_not_found_error(create_mock_s3):
    with pytest.raises(ValueError):
        dim_address_data_frame('non_existent_file')


def test_dim_address_data_frame_empty_table_name_error():
    with pytest.raises(ValueError):
        dim_address_data_frame('')
