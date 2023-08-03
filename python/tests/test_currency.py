import pytest
import pandas as pd
import boto3
from moto import mock_s3
from src.dim_currency import (dim_currency_data_frame, create_and_push_parquet, main)
import tempfile

# Set up the mock S3 environment and create a CSV for testing
@pytest.fixture
def create_mock_s3():
    with mock_s3():
        s3 = boto3.client('s3', region_name='eu-west-2')
        s3.create_bucket(Bucket='ingested-data-vox-indicium', CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        s3.create_bucket(Bucket='processed-data-vox-indicium', CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        csv_data = "2,USD,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000\n"
        s3.put_object(Bucket='ingested-data-vox-indicium', Key='currency.csv', Body=csv_data)
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

def test_create_parquet_content(create_mock_s3):
    expected_df = pd.DataFrame({
        "currency_id": [1],
        "currency_code": ["EUR"],
        "currency_name": ["Euro"]
    })

    create_and_push_parquet(expected_df, 'currency')

    s3 = boto3.client('s3', region_name='eu-west-2')

    # Check if the file was created in the S3 bucket 
    response = s3.get_object(Bucket='processed-data-vox-indicium', Key='currency.parquet')
    
    parquet_file = response['Body'].read()

    # Write the retrieved content to a temporary file
    retrieved_parquet_path = tempfile.mktemp(suffix='.parquet')
    with open(retrieved_parquet_path, 'wb') as temp_file:
        temp_file.write(parquet_file)
    
     # Read the parquet file with pandas
    read_parquet = pd.read_parquet(retrieved_parquet_path)

    # Compare both DataFrames
    pd.testing.assert_frame_equal(expected_df, read_parquet)


def test_main(create_mock_s3):

    main()

    # Verify if the file has been transferred to the final bucket
    s3 = boto3.client('s3', region_name='eu-west-2')

    response = s3.get_object(Bucket='processed-data-vox-indicium', Key='dim_currency.parquet')
   
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
