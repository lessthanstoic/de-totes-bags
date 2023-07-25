import boto3
from pprint import pprint
import pandas as pd


def retrieve_previous_data(table_name):
    
    file_name = table_name + ".csv"

    s3 = boto3.client('s3')

    file = s3.get_object(Bucket='ingested-data-vox-indicium', Key=file_name)

    resulting_df = pd.read_csv(file['Body'])

    return resulting_df