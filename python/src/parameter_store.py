import boto3


def save_to_parameter_store(parameter_name, value):
    ssm = boto3.client('ssm')
    ssm.put_parameter(
        Name=parameter_name,
        Value=value,
        Type='String',
        Overwrite=True
    )


def load_from_parameter_store(parameter_name):
    ssm = boto3.client('ssm')
    try:
        response = ssm.get_parameter(Name=parameter_name, WithDecryption=False)
    except ssm.exceptions.ParameterNotFound as ep:
        # We need access to ssm client to access the specific exceptions
        # ieally this logic would be handled in the main function
        # but then we'd need to initialise another connection
        # to the boto3.client('ssm') so we're returning here -
        # - though this makes it not pep8 compliant
        return '1901-01-01'
        ep
    except Exception as e:
        raise e
    return response['Parameter']['Value']
