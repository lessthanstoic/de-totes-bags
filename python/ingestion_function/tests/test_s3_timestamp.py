from python.ingestion_function.src.s3_timestamp import get_s3_timestamp
from moto import mock_s3
import boto3
from pytest import raises


@mock_s3
def create_mock_s3():
    mock_client = boto3.client('s3')
    mock_client.create_bucket(Bucket='ingested-data-vox-indicium',
                              CreateBucketConfiguration={
                                  'LocationConstraint': 'eu-west-2', })
    with open('python/ingestion_function/tests/mock_timestamp.txt', 'rb') as data:  # noqa: E501
        mock_client.put_object(Bucket='ingested-data-vox-indicium',
                               Body=data,
                               Key='mock_timestamp.txt')


@mock_s3
def test_read_datetime_from_s3_text_file():
    create_mock_s3()
    ts = get_s3_timestamp("mock_timestamp.txt")
    assert ts == '1901-01-01 01:01:01.001'


@mock_s3
def test_retrieve_data_returns_default_value_if_no_file_in_bucket():
    create_mock_s3()

    assert get_s3_timestamp('no_file') == '1901-01-01 01:01:01.001'


@mock_s3
def test_retrieve_data_error_handling_with_empty_string():
    create_mock_s3()
    with raises(ValueError, match="No input name"):
        get_s3_timestamp('')
