import pytest
import pandas as pd
import boto3
from moto import mock_s3
from python.transformation_function.src.fact_sales_order import (
    fact_sales_order_data_frame, create_and_push_parquet, main)
# Set up the mock S3 environment and create a CSV for testing


@pytest.fixture
def create_mock_s3():
    with mock_s3():
        s3 = boto3.client('s3', region_name='eu-west-2')
        s3.create_bucket(Bucket='ingested-data-vox-indicium',
                         CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        s3.create_bucket(Bucket='processed-data-vox-indicium',
                         CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        csv_data = "sales_order_id,created_at,last_updated,design_id,staff_id,counterparty_id,units_sold,unit_price,currency_id,agreed_delivery_date,agreed_payment_date,agreed_delivery_location_id\n1,2022-11-03 14:20:52.186000,2022-11-03 14:20:52.186000,9,16,18,84754,2.43,3,2022-11-10,2022-11-03,4\n"
        s3.put_object(Bucket='ingested-data-vox-indicium',
                      Key='sales_order.csv', Body=csv_data)
        yield
# Test the sales_order_data_frame function


def test_fact_sales_order_data_frame(create_mock_s3):
    result = fact_sales_order_data_frame('sales_order')
    assert isinstance(result, pd.DataFrame)
    # Check the number of rows
    assert result.shape[0] == 1


def test_sales_order_data_frame_invalid_table_name_type_error():
    with pytest.raises(TypeError):
        # Non-string input
        fact_sales_order_data_frame(123)


def test_sales_order_data_frame_file_not_found_error():
    with pytest.raises(ValueError):
        fact_sales_order_data_frame('non_existent_file')
# Test error for empty table name


def test_sales_order_data_empty_table_name_error():
    with pytest.raises(ValueError):
        fact_sales_order_data_frame('')
# Test if the fact_sales_order_data_frame return the right columns


def test_fact_sales_order_data_frame_with_correct_columns(create_mock_s3):
    result = fact_sales_order_data_frame('sales_order')
    expect_sales = ['sales_record_id', 'sales_order_id', 'created_date', 'created_time', 'last_updated_date',
                    'last_updated_time', 'design_id', 'sales_staff_id', 'counterparty_id', 'units_sold',
                    'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'
                    ]
    assert all(column in result.columns for column in expect_sales)
# Test if the fact_sales_order_data_frame return the right content


def test_fact_sales_order_data_frame_values(create_mock_s3):
    result = fact_sales_order_data_frame('sales_order')
    print( result.values.tolist())
    expect_sales_content = [[1, 1, '2022-11-03', '14:20:52.186000', '2022-11-03', '14:20:52.186000', 16, 18, 84754, 2.43, 3, 9, '2022-11-03', '2022-11-10', 4]]
    assert result.values.tolist() == expect_sales_content
# Test the create_and_push_parquet function


def test_create_and_push_parquet(create_mock_s3):
    df = pd.DataFrame({'dummy': [1]})
    create_and_push_parquet(df, 'sales_order')
    s3 = boto3.client('s3', region_name='eu-west-2')
    # Check if the file was created in the S3 bucket
    response1 = s3.get_object(
        Bucket='processed-data-vox-indicium', Key='sales_order.parquet')
    assert response1['ResponseMetadata']['HTTPStatusCode'] == 200
# Test the main function


def test_main(create_mock_s3):
    main()
    # Check if the file was transferred in the final bucket
    s3 = boto3.client('s3', region_name='eu-west-2')
    response1 = s3.get_object(
        Bucket='processed-data-vox-indicium', Key='fact_sales_order.parquet')
    assert response1['ResponseMetadata']['HTTPStatusCode'] == 200
