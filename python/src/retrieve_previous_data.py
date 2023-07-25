import boto3
from pprint import pprint
import pandas as pd
from botocore.exceptions import ClientError


def retrieve_previous_data(table_name):
    try:
        file_name = table_name + ".csv"

        s3 = boto3.client('s3')

        file = s3.get_object(Bucket='ingested-data-vox-indicium', Key=file_name)

        resulting_df = pd.read_csv(file['Body'])

        return resulting_df
    
    except ClientError as e:

        if e.response['Error']['Code'] == 'NoSuchKey':
            return None