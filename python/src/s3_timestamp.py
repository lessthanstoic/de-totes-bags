import boto3
from botocore.exceptions import ClientError


def get_s3_timestamp(filename):
    try:
        if len(filename)==0:
            raise ValueError("No input name")
        
        #connect to the s3
        s3 = boto3.client('s3')

        #get the object with the input name
        file = s3.get_object(Bucket='ingested-data-vox-indicium', Key=filename)

        f = open(file, "r")
        
        return f
    
    except ClientError as e:
        #catches the error if the user tap a non-existent table name
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise ValueError(f"The file {filename} does not exist")
        
    except TypeError as e:
        #catches the error if the user tap an incorrect input 
        raise TypeError("Function must take a string input")
