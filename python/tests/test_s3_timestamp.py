from src.s3_timestamp import get_s3_timestamp
from moto import mock_s3
import boto3
from pytest import raises


@mock_s3
def create_mock_s3():
    mock_client = boto3.client('s3')
    mock_client.create_bucket(Bucket='ingested-data-vox-indicium',
                              CreateBucketConfiguration={
                                  'LocationConstraint': 'eu-west-2', })
    with open('python/tests/mock_timestamp.txt', 'rb') as data:
        mock_client.put_object(Bucket='ingested-data-vox-indicium',
                               Body=data,
                               Key='mock_timestamp.txt')


@mock_s3
def test_read_datetime_from_s3_text_file():
    create_mock_s3()
    ts = get_s3_timestamp("mock_timestamp.txt")
    assert ts == '1901-01-01 01:01:01.001'


@mock_s3
def test_retrieve_data_error_handling():
    create_mock_s3()
    with raises(ValueError, match="The file no_file does not exist"):
        get_s3_timestamp('no_file')


@mock_s3
def test_retrieve_data_error_handling_with_empty_string():
    create_mock_s3()
    with raises(ValueError, match="No input name"):
        get_s3_timestamp('')
