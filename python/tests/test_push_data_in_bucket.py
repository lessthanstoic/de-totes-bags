from python.src.push_data_in_bucket import push_data_in_bucket
from python.src.push_data_in_bucket import log_changes_to_db
from moto import mock_s3, mock_logs
from pprint import pprint
import boto3
from pytest import raises


@mock_s3
def create_s3_mock_bucket():
    client = boto3.client("s3")
    client.create_bucket(Bucket="ingested-data-vox-indicium",
                         CreateBucketConfiguration={
                             'LocationConstraint': 'eu-west-2'})


@mock_s3
def test_push_data_in_bucket_function():

    create_s3_mock_bucket()

    file_path = 'python/tests/test_file.csv'
    file_name = 'test_file.csv'

    push_data_in_bucket(file_path, file_name)

    client = boto3.client("s3")

    result = client.list_objects(
        Bucket="ingested-data-vox-indicium")['Contents'][0]['Key']

    assert file_name in result


@mock_s3
def test_error_handling_for_if_file_path_is_invalid():

    create_s3_mock_bucket()

    file_path = 'python/test_file.csv'
    file_name = 'test_file.csv'

    with raises(FileNotFoundError):
        push_data_in_bucket(file_path, file_name)


@mock_logs
def create_mock_log_group():
    mock_client = boto3.client('logs')
    mock_client.create_log_group(
        logGroupName='MyLogger'
    )
    mock_client.create_log_stream(
        logGroupName='MyLogger', logStreamName='test_stream')


@mock_logs
def test_log_changes():
    file_path = 'python/tests/test_file.csv'
    file_name = 'test_file.csv'

    create_mock_log_group()

    mock_client = boto3.client('logs')

    log_changes_to_db(file_path, file_name)

    response = mock_client.get_log_events(
        logGroupName='MyLogger',
        logStreamName='test_stream'
    )

    pprint(response)

    result = response['events'][0]['message']

    assert result == 'Number of changes made to test_file.csv: 2'
