import botocore
from botocore.exceptions import (EndpointConnectionError, NoCredentialsError, ClientError)
import boto3
from moto import mock_s3
from pprint import pprint

def push_parquet_file(file_path):
    """
    Function to push a Parquet file to an Amazon S3 bucket.

    param file_path: Path to the file to be uploaded.
    return: A success message if the file is uploaded, or an error message if an exception occurs.
    """
    
    #connect to the s3
    s3 = boto3.client('s3')
    
    try:         
        # Extract the file name from the provided file path
        file_name = file_path.split('/')[-1]

        # Open the file in binary reading mode
        with open(file_path, 'rb') as data:
            # Put the file object into the specific S3 bucket
            s3.put_object(Bucket='processed-data-vox-indicium', Body=data, Key=file_name)
            print(f"File {file_name} successfully uploaded to S3 bucket") 

    except FileNotFoundError:
        # Return error message if the file is not found
        return f"Error: File {file_path} not found."
    except EndpointConnectionError:
        # Return error message if unable to connect to the S3 endpoint
        return "Error: Unable to connect to the S3 endpoint"
    except NoCredentialsError:
        # Return error message if no credentials are found
        return "Error: No credentials found."
    except ClientError as e:
        # Return error message if a client error occurs
        return f"Error: {e}"
