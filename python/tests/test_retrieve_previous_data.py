from src.retrieve_previous_data import retrieve_previous_data
import pandas as pd
from moto import mock_s3
import boto3
from pprint import pprint
from pytest import raises


@mock_s3
def create_mock_s3():
    mock_client = boto3.client('s3')
    mock_client.create_bucket(Bucket='ingested-data-vox-indicium', CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2',
    })
    with open('python/tests/example.csv', 'rb') as data:
        mock_client.put_object(Bucket='ingested-data-vox-indicium', Body=data, Key='example.csv')


@mock_s3
def test_retrieve_data_reads_a_csv_file_from_bucket_and_converts_to_dataframe():

    create_mock_s3()

    result = retrieve_previous_data('example')

    expected_result = pd.DataFrame({'user_id': [1, 2, 3, 4], 'name': ['Andrei', 'Michael', 'Mark', 'Simon'], 'password': ['Password123', 'Liverpool654', 'Edward34', '!!QWasd']})

    pd.testing.assert_frame_equal(result, expected_result)

@mock_s3
def test_retrieve_data_error_handling():

    create_mock_s3()

    with raises(ValueError, match="The file no_file does not exist"): 
        retrieve_previous_data('no_file')

@mock_s3
def test_retrieve_data_error_handling_with_empty_string():

    create_mock_s3()

    with raises(ValueError, match="No input name"): 
        retrieve_previous_data('')

@mock_s3
def test_retrieve_data_error_handling_with_wrong_input():

    create_mock_s3()

    with raises(TypeError, match="Function must take a string input"): 
        retrieve_previous_data(4875684)
    
    with raises(TypeError, match="Function must take a string input"): 
        retrieve_previous_data(True)

    with raises(TypeError, match="Function must take a string input"): 
        retrieve_previous_data(['test', 233, 'test'])