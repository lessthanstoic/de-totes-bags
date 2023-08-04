import pytest
import pandas as pd
import boto3
from moto import mock_s3
from python.transformation_function.src.dim_staff import (
    dim_staff_data_frame,
    create_and_push_parquet,
    main)
import tempfile


# Set up the mock S3 environment and create the CSV for testing
@pytest.fixture
def create_mock_s3():
    with mock_s3():
        s3 = boto3.client('s3', region_name='eu-west-2')
        s3.create_bucket(Bucket='ingested-data-vox-indicium',
                         CreateBucketConfiguration={'LocationConstraint':
                                                    'eu-west-2'})
        s3.create_bucket(Bucket='processed-data-vox-indicium',
                         CreateBucketConfiguration={'LocationConstraint':
                                                    'eu-west-2'})
        csv_data = "staff_id,first_name,last_name,department_id,email_address,created_at,last_updated\n1,Jeremie,Franey,2,jeremie.franey@terrifictotes.com,2022-11-03 14:20:51.563000,2022-11-03 14:20:51.563000\n"  # noqa: E501
        csv_data2 = "department_id,department_name,location,manager,created_at,last_updated\n2,Purchasing,Manchester,Naomi Lapaglia,2022-11-03 14:20:49.962000,2022-11-03 14:20:49.962000\n"  # noqa: E501
        s3.put_object(Bucket='ingested-data-vox-indicium',
                      Key='staff.csv', Body=csv_data)
        s3.put_object(Bucket='ingested-data-vox-indicium',
                      Key='department.csv', Body=csv_data2)
        yield


# Test the dim_staff_data_frame function
def test_dim_staff_data_frame(create_mock_s3):
    result = dim_staff_data_frame('staff', 'department')
    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] == 1


def test_dim_staff_data_frame_invalid_table_name_type_error():
    with pytest.raises(TypeError):
        dim_staff_data_frame(123)


def test_dim_staff_data_frame_file_not_found_error(create_mock_s3):
    with pytest.raises(ValueError):
        dim_staff_data_frame('non_existent_file', 'address')

# Test error for empty table name


def test_dim_staff_data_frame_empty_table_name_error():
    with pytest.raises(ValueError):
        dim_staff_data_frame('', 'department')

# Test the create_and_push_parquet function


def test_if_the_parquet_file_was_created_in_the_process_bucket(create_mock_s3):

    df = pd.DataFrame({'dummy': [1]})

    create_and_push_parquet(df, 'dim_staff')

    s3 = boto3.client('s3', region_name='eu-west-2')

    # Check if the file was created in the S3 bucket
    response = s3.get_object(Bucket='processed-data-vox-indicium',
                             Key='dim_staff.parquet')

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


def test_parquet_content_from_process_bucket(create_mock_s3):
    # Create expected DataFrame
    expected_df = pd.DataFrame({
        "staff_id": [1],
        "first_name": ["Jeremie"],
        "last_name": ["Franey"],
        "department_name": ["Purchasing"],
        "location": ["Manchester"],
        "email_address": ["jeremie.franey@terrifictotes.com"]
        })

    create_and_push_parquet(expected_df, 'dim_staff')

    s3 = boto3.client('s3', region_name='eu-west-2')
    # Retrieve the parquet file from S3 bucket
    response = s3.get_object(Bucket='processed-data-vox-indicium',
                             Key='dim_staff.parquet')
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

    response = s3.get_object(Bucket='processed-data-vox-indicium',
                             Key='dim_staff.parquet')

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
