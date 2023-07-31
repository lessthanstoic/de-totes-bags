import pytest
import pandas as pd
import boto3
import os
from moto import mock_s3
from src.sales_order import (sales_order_data_frame, create_parquet, main)

# Set up the mock S3 environment and create a CSV for testing
@pytest.fixture
def create_mock_s3():
    with mock_s3():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='ingested-data-vox-indicium')
        csv_data = "1,2023-07-31 12:00:00.000,2023-07-31 12:00:00.000,1,1,1,1,100.0,1,2023-07-31,2023-07-31,1\n"
        s3.put_object(Bucket='ingested-data-vox-indicium', Key='sales_order.csv', Body=csv_data)
        yield

# Test the sales_order_data_frame function
def test_sales_order_data_frame(create_mock_s3):
    result = sales_order_data_frame('sales_order')
    assert isinstance(result, pd.DataFrame)
    # Check the number of rows
    assert result.shape[0] == 1  

def test_sales_order_data_frame_invalid_table_name_type_error():
    with pytest.raises(TypeError):
        # Non-string input
        sales_order_data_frame(123) 
    
def test_sales_order_data_frame_file_not_found_error():
    with pytest.raises(ValueError):
        sales_order_data_frame('non_existent_file')

# Test error for empty table name
def test_sales_order_data_empty_table_name_error():
    with pytest.raises(ValueError):
        sales_order_data_frame('')

# Test the create_parquet function
def test_create_parquet():
    df = pd.DataFrame({
        'sales_order_id': [1],
        'created_date': ['2023-07-31'],
        'created_time': ['12:00:00'],
        'last_updated_date': ['2023-07-31'],
        'last_updated_time': ['12:00:00'],
        'design_id': [1],
        'sales_staff_id': [1],
        'counterparty_id': [1],
        'units_sold': [1],
        'unit_price': [100.0],
        'currency_id': [1],
        'agreed_delivery_date': ['2023-07-31'],
        'agreed_payment_date': ['2023-07-31'],
        'agreed_delivery_location_id': [1]
    })
    create_parquet(df, 'test_table_name')
    # Check if the file exists
    assert os.path.exists('test_table_name.parquet')  

def test_parquet_content():
    df = pd.DataFrame({
        'sales_order_id': [1],
        'created_date': ['2023-07-31'],
        'created_time': ['12:00:00'],
        'last_updated_date': ['2023-07-31'],
        'last_updated_time': ['12:00:00'],
        'design_id': [1],
        'sales_staff_id': [1],
        'counterparty_id': [1],
        'units_sold': [1],
        'unit_price': [100.0],
        'currency_id': [1],
        'agreed_delivery_date': ['2023-07-31'],
        'agreed_payment_date': ['2023-07-31'],
        'agreed_delivery_location_id': [1]
    })
    create_parquet(df, 'test_table_content')
    read_parquet = pd.read_parquet('test_table_content.parquet')
    pd.testing.assert_frame_equal(df, read_parquet)

# Test the main function
def test_main(create_mock_s3):
    main()
    # Check if the file exists
    assert os.path.exists('parquet_files/sales_order.parquet')  
