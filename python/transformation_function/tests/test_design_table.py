import pytest
import pandas as pd
import boto3
from moto import mock_s3
from python.transformation_function.src.dim_design_table import (
    dim_design_table_data_frame)


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
        csv_data = "design_id,created_at,design_name,file_location,file_name,last_updated\n8,2022-11-03 14:20:49.962000,Wooden,/usr,wooden-20220717-npgz.json,2022-11-03 14:20:49.962000\n"  # noqa: E501
        s3.put_object(
            Bucket='ingested-data-vox-indicium',
            Key='design.csv', Body=csv_data)
        yield


# Test the design_table_data_frame function
def test_design_table_data_frame(create_mock_s3):
    result = dim_design_table_data_frame('design')
    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] == 1


def test_design_table_data_frame_invalid_table_name_type_error():
    with pytest.raises(TypeError):
        dim_design_table_data_frame(123)


def test_design_table_data_frame_file_not_found_error(create_mock_s3):
    with pytest.raises(ValueError):
        dim_design_table_data_frame('non_existent_file')


# Test error for empty table name
def test_design_table_data_frame_empty_table_name_error():
    with pytest.raises(ValueError):
        dim_design_table_data_frame('')


# Test if the fact_sales_order_data_frame return the right columns
def test_fact_sales_order_data_frame_with_correct_columns(create_mock_s3):
    result = dim_design_table_data_frame('design')
    expect = ['design_id', 'design_name', 'file_location', 'file_name']
    assert all(column in result.columns for column in expect)


# Test if the fact_sales_order_data_frame return the right value
def test_fact_sales_order_data_frame_values(create_mock_s3):
    result = dim_design_table_data_frame('design')
    print(result)
    expect_values = [[8, 'Wooden', '/usr', 'wooden-20220717-npgz.json']]
    assert result.values.tolist() == expect_values
