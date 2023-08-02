'''
import pytest
import pandas as pd
import boto3
import os
from moto import mock_s3
from src.dim_counterparty import (dim_counterparty_data_frame, create_parquet, main)

# Set up the mock S3 environment and create a CSV for testing
@pytest.fixture
def create_mock_s3():
    with mock_s3():
        s3 = boto3.client('s3', region_name='eu-west-2')
        s3.create_bucket(Bucket='ingested-data-vox-indicium')
        csv_data = "1,Fahey and Sons,15,Micheal Toy,Mrs. Lucy Runolfsdottir,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000\n"
        s3.put_object(Bucket='ingested-data-vox-indicium', Key='sales_order.csv', Body=csv_data)
        yield

# Test the dim_counterparty_data_frame function
def test_dim_counterparty_data_frame(create_mock_s3):
    result = dim_counterparty_data_frame('sales_order')
    assert isinstance(result, pd.DataFrame)
    # Check the number of rows
    assert result.shape[0] == 1  

def test_dim_counterparty_data_frame_invalid_table_name_type_error():
    with pytest.raises(TypeError):
        # Non-string input
        dim_counterparty_data_frame(123) 
    
def test_dim_counterparty_data_frame_file_not_found_error():
    with pytest.raises(ValueError):
        dim_counterparty_data_frame('non_existent_file')

# Test error for empty table name
def test_dim_counterparty_data_frame_empty_table_name_error():
    with pytest.raises(ValueError):
        dim_counterparty_data_frame('')

# Test the create_parquet function
def test_create_parquet():
    df = pd.DataFrame({
        "counterparty_id": [1], 
        "counterparty_legal_name": ["Fahey and Sons"],
        "legal_address_id": [2], 
        "commercial_contact": ["Micheal Toy"],
        "delivery_contact": ["Mrs. Lucy Runolfsdottirr"], 
        "created_date": ["2022-11-03"], 
        "created_time": ["14:20:51"],
        "last_updated_date": ["2022-11-03"],
        "last_updated_time": ["14:20:51"]
        })
    create_parquet(df, 'test_table_name')
    # Check if the file exists
    assert os.path.exists('test_table_name.parquet')  

def test_parquet_content():
    df = pd.DataFrame({
        "counterparty_id": [1], 
        "counterparty_legal_name": ["Fahey and Sons"],
        "legal_address_id": [2], 
        "commercial_contact": ["Micheal Toy"],
        "delivery_contact": ["Mrs. Lucy Runolfsdottirr"], 
        "created_date": ["2022-11-03"], 
        "created_time": ["14:20:51"],
        "last_updated_date": ["2022-11-03"],
        "last_updated_time": ["14:20:51"]
        })
    create_parquet(df, 'test_table_content')
    read_parquet = pd.read_parquet('test_table_content.parquet')
    pd.testing.assert_frame_equal(df, read_parquet)

# Test the main function
def test_main(create_mock_s3):
    main()
    # Check if the file exists
    assert os.path.exists('parquet_files/counterparty.parquet')  
'''
#tests for the new version

import pytest
import pandas as pd
import boto3
from moto import mock_s3
from python.transformation_function.src.dim_counterparty import (dim_counterparty_data_frame, create_parquet, push_parquet_file, main)
import tempfile

# Set up the mock S3 environment and create a CSV for testing
@pytest.fixture
def create_mock_s3():
    with mock_s3():
        s3 = boto3.client('s3', region_name='eu-west-2')
        s3.create_bucket(Bucket='ingested-data-vox-indicium', CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        s3.create_bucket(Bucket='processed-data-vox-indicium', CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        csv_data = "1,Fahey and Sons,15,Micheal Toy,Mrs. Lucy Runolfsdottir,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000\n"
        s3.put_object(Bucket='ingested-data-vox-indicium', Key='counterparty.csv', Body=csv_data)
        yield

# Test the dim_counterparty_data_frame function
def test_dim_counterparty_data_frame(create_mock_s3):
    result = dim_counterparty_data_frame('counterparty')
    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] == 1

def test_dim_counterparty_data_frame_invalid_table_name_type_error():
    with pytest.raises(TypeError):
        dim_counterparty_data_frame(123)

def test_dim_counterparty_data_frame_file_not_found_error(create_mock_s3):
    with pytest.raises(ValueError):
        dim_counterparty_data_frame('non_existent_file')

# Test error for empty table name
def test_dim_counterparty_data_frame_empty_table_name_error():
    with pytest.raises(ValueError):
        dim_counterparty_data_frame('')

# Test the create_parquet function
def test_create_parquet(create_mock_s3):
    df = pd.DataFrame({
        "counterparty_id": [1],
        "counterparty_legal_name": ["Fahey and Sons"],
        "legal_address_id": [2],
        "commercial_contact": ["Micheal Toy"],
        "delivery_contact": ["Mrs. Lucy Runolfsdottirr"],
        "created_date": ["2022-11-03"],
        "created_time": ["14:20:51"],
        "last_updated_date": ["2022-11-03"],
        "last_updated_time": ["14:20:51"]
        })
    create_parquet(df, 'counterparty')
    s3 = boto3.client('s3', region_name='eu-west-2')

    # Check if the file was created in the S3 bucket 
    response = s3.get_object(Bucket='ingested-data-vox-indicium', Key='counterparty.parquet')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

def test_push_parquet_file(create_mock_s3):
    
    s3 = boto3.client('s3', region_name='eu-west-2')
    s3.put_object(Bucket='ingested-data-vox-indicium', Key='counterparty.parquet', Body=b'test')
    push_parquet_file('counterparty')
    # Check if the parquet file was send to the process bucket
    response = s3.get_object(Bucket='processed-data-vox-indicium', Key='counterparty.parquet')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

def test_parquet_content_in_processed_bucket(create_mock_s3):
    # Create expected DataFrame
    expected_df = pd.DataFrame({
        "counterparty_id": [1],
        "counterparty_legal_name": ["Fahey and Sons"],
        "legal_address_id": [2],
        "commercial_contact": ["Micheal Toy"],
        "delivery_contact": ["Mrs. Lucy Runolfsdottirr"],
        "created_date": ["2022-11-03"],
        "created_time": ["14:20:51"],
        "last_updated_date": ["2022-11-03"],
        "last_updated_time": ["14:20:51"]
    })

    # Write DataFrame to a temporary Parquet file
    temp_parquet_path = tempfile.mktemp(suffix='.parquet')
    expected_df.to_parquet(temp_parquet_path)

    # Read the content of the Parquet file into bytes
    with open(temp_parquet_path, 'rb') as temp_file:
        parquet_content = temp_file.read()

    # Put the parquet file into the S3 bucket
    s3 = boto3.client('s3', region_name='eu-west-2')
    s3.put_object(Bucket='processed-data-vox-indicium', Key='counterparty.parquet', Body=parquet_content)

    # Retrieve the parquet file from S3 bucket
    response = s3.get_object(Bucket='processed-data-vox-indicium', Key='counterparty.parquet')
    parquet_file = response['Body'].read()

    # Write the retrieved content to a temporary file
    retrieved_parquet_path = tempfile.mktemp(suffix='.parquet')
    with open(retrieved_parquet_path, 'wb') as temp_file:
        temp_file.write(parquet_file)

    # Read the parquet file with pandas
    read_parquet = pd.read_parquet(retrieved_parquet_path)

    # Compare both DataFrames
    pd.testing.assert_frame_equal(expected_df, read_parquet)

# Test the main function
def test_main(create_mock_s3):
    main()
    # Check if the file was transferred in the final bucket
    s3 = boto3.client('s3', region_name='eu-west-2')
    response = s3.get_object(Bucket='processed-data-vox-indicium', Key='counterparty.parquet')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
