from python.src.secret_login import retrieve_secret_details
import boto3
from moto import mock_secretsmanager
# import os
import pytest
from botocore.exceptions import ClientError
# from pprint import pprint
import json


# @pytest.fixture(scope="function")
# def aws_credentials():
#     """Mocked AWS Credentials for moto."""
#     os.environ["AWS_ACCESS_KEY_ID"] = "testing"
#     os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
#     os.environ["AWS_SECURITY_TOKEN"] = "testing"
#     os.environ["AWS_SESSION_TOKEN"] = "testing"
#     os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


# @pytest.fixture(scope="session")
# def client():
#     conn = boto3.client("secretsmanager", region_name="eu-west-2")
#     yield conn


# @mock_secretsmanager
# def test_retrieve_correct_secret(client):
#     # ARRANGE
#     secret = str({"username": "jeff", "password": "sdfsdf"})
#     client.create_secret(
#        Name="one", SecretString=secret
#     )
#     # ACT
#     response = retrieve_secret_details("one")
#     assert response['username'] == 'jeff'
#     assert response['password'] == 'sdfsdf'


# @mock_secretsmanager
# def test_raises_client_error(client):
#     # ARRANGE
#     secret = str({"username": "jeff", "password": "sdfsdf"})
#     client.create_secret(
#        Name="one", SecretString=secret
#     )
#     # ACT / ASSERT
#     with pytest.raises(ClientError):
#         retrieve_secret_details("on")

@mock_secretsmanager
def import_secret_to_mock(secret_id):
    mock_client = boto3.client('secretsmanager')
    mock_client.create_secret(Name=secret_id, SecretString=json.dumps(
        {"username": 'test_user', "password": 'test_password'}))


@mock_secretsmanager
def test_retrieves_correct_secret():

    import_secret_to_mock('test_secret')

    answer = retrieve_secret_details('test_secret')

    assert answer == {'password': 'test_password', 'username': 'test_user'}


@mock_secretsmanager
def test_raises_client_error():

    import_secret_to_mock('test_secret')

    with pytest.raises(ClientError):
        retrieve_secret_details('no_secret')

    pass


"""
ISSUE IDENTIFIED - In testing, secrets have quote marks
    surrounding their values to convert them to json format.

    Our actual secret didn't have quote marks around the values.
    This causes an error when we attempt when doing json.loads

    SOLUTION IMPLEMENTED:
    - created a new secret with a json format that contains all
    database information called 'Totesys-Access. This allows the
    removal of hardcoded credentials from data_capture file as well"""
