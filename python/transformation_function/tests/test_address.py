import pytest
import pandas as pd
import boto3
from moto import mock_s3
from python.transformation_function.src.dim_address import (dim_address_data_frame, create_parquet, push_parquet_file, main)

# Set up the mock S3 environment and create a CSV for testing
@pytest.fixture
def create_mock_s3():
    with mock_s3():
        s3 = boto3.client('s3', region_name='eu-west-2')
        s3.create_bucket(Bucket='ingested-data-vox-indicium', CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        s3.create_bucket(Bucket='processed-data-vox-indicium', CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        csv_data = "1,EUR,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000\n"
        s3.put_object(Bucket='ingested-data-vox-indicium', Key='address.csv', Body=csv_data)
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

def test_create_parquet(create_mock_s3):
    df = pd.DataFrame({
        "address_id": [1],
        "address_line_1": ["6826 Herzog Via"],
        "address_line_2": [""],
        "district": ["Avon"],
        "city": ["New Patienceburgh"],
        "postal_code": ["28441"],
        "country": ["Turkey"],
        "phone": ["1803 637401"],
        "created_date": ["2022-11-03"],
        "created_time": ["14:20:49"],
        "last_updated_date": ["2022-11-03"],
        "last_updated_time": ["14:20:49"]
    })
    create_parquet(df, 'address')
    s3 = boto3.client('s3', region_name='eu-west-2')
    # Check if the file was created in the S3 bucket 
    response = s3.get_object(Bucket='ingested-data-vox-indicium', Key='address.parquet')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

def test_push_parquet_file(create_mock_s3):
    s3 = boto3.client('s3', region_name='eu-west-2')
    s3.put_object(Bucket='ingested-data-vox-indicium', Key='address.parquet', Body=b'test')
    push_parquet_file('address')
    # Check if the parquet file was sent to the process bucket
    response = s3.get_object(Bucket='processed-data-vox-indicium', Key='address.parquet')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

def test_main(create_mock_s3):
    main()
    # Verify if the file has been transferred to the final bucket
    s3 = boto3.client('s3', region_name='eu-west-2')
    response = s3.get_object(Bucket='processed-data-vox-indicium', Key='address.parquet')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
