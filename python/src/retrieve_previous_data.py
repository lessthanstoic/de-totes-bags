import boto3
from pprint import pprint
import pandas as pd
from botocore.exceptions import ClientError

"""

"""
def retrieve_previous_data(table_name):
    """
    
    """

    try:
        if len(table_name)==0:
            raise ValueError("No input name")
        
        #add csv extension
        file_name = table_name + ".csv" 

        #connect to the s3
        s3 = boto3.client('s3')

        file = s3.get_object(Bucket='ingested-data-vox-indicium', Key=file_name)

        resulting_df = pd.read_csv(file['Body'])

        return resulting_df
    
    except ClientError as e:

        if e.response['Error']['Code'] == 'NoSuchKey':
            raise ValueError(f"The file {table_name} does not exist")
        
    except TypeError as e:
        
        raise TypeError("Function must take a string input")
