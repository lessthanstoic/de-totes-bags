from src.parameter_store import (
    save_to_parameter_store,
    load_from_parameter_store
)
import boto3
from moto import mock_ssm
import os
import pytest


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@mock_ssm
def test_add_to_ssm():
    # ARRANGE
    ssm = boto3.client('ssm')
    # ACT
    save_to_parameter_store("foo", "bar")
    # ASSERT
    param = ssm.get_parameter(Name="foo")
    assert param['Parameter']['Value'] == "bar"


@mock_ssm
def test_get_from_ssm():
    # ARRANGE
    ssm = boto3.client('ssm')
    ssm.put_parameter(
        Name="foo",
        Value="bar",
        Type="String",
    )
    foo = load_from_parameter_store('foo')
    assert foo == "bar"


@mock_ssm
def test_exception_from_ssm():
    # ARRANGE
    ssm = boto3.client('ssm')
    ssm.put_parameter(
        Name="foo",
        Value="bar",
        Type="String",
    )
    with pytest.raises(Exception):
        load_from_parameter_store('')


@mock_ssm
def test_paramnotfound_exception_from_ssm():
    # ARRANGE
    ssm = boto3.client('ssm')
    ssm.put_parameter(
        Name="foo",
        Value="bar",
        Type="String",
    )
    with pytest.raises(ssm.exceptions.ParameterNotFound):
        load_from_parameter_store('fo')
