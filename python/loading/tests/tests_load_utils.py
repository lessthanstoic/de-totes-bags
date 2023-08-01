from python.loading.src.load_utils import getFileFromS3
# from python.src.push_data_in_bucket import log_changes_to_db
from moto import mock_s3, mock_logs
import boto3
from pytest import raises


@mock_s3
def create_mock_s3():
    mock_client = boto3.client('s3')
    mock_client.create_bucket(Bucket='processed-data-vox-indicium',
                              CreateBucketConfiguration={
                                  'LocationConstraint': 'eu-west-2', })
    with open('python/loading/tests/fact_sales.parquet', 'rb') as data:
        mock_client.put_object(Bucket='ingested-data-vox-indicium',
                               Body=data,
                               Key='fact_sales.parquet')


@mock_s3
def test_can_read_file_from_s3_bucket():
    create_mock_s3()
    file = getFileFromS3('processed-data-vox-indicium', 'fact_sales.parquet')
    assert file.name == 'fact_sales.parquet'