from src.secret_login import (
    retrieve_secret_details
)
import boto3
from moto import mock_secretsmanager
import os
import pytest
from botocore.exceptions import ClientError


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="session")
def client():
    conn = boto3.client("secretsmanager", region_name="eu-west-2")
    yield conn


@mock_secretsmanager
def test_retrieve_correct_secret(client):
    # ARRANGE
    secret = str({"username": "jeff", "password": "sdfsdf"})
    client.create_secret(
       Name="one", SecretString=secret
    )
    # ACT
    response = retrieve_secret_details("one")
    # ASSERT
    assert response['username'] == 'jeff'
    assert response['password'] == 'sdfsdf'


@mock_secretsmanager
def test_raises_client_error(client):
    # ARRANGE
    secret = str({"username": "jeff", "password": "sdfsdf"})
    client.create_secret(
       Name="one", SecretString=secret
    )
    # ACT / ASSERT
    with pytest.raises(ClientError):
        retrieve_secret_details("on")
