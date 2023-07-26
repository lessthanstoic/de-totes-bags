import boto3
from pprint import pprint
import pandas as pd
from botocore.exceptions import ClientError

"""
This module reads .csv files from our ingestion bucket, and converts them to a pandas dataframe.
This module contains one function:
retrieve_previous_data
"""
def retrieve_previous_data(table_name):
    """
    This function reads a .csv file from our ingestion bucket.
    Arguments:
    table_name (string) - represents the name of a table in our database.
    Output:
    resulting_df (DataFrame) - outputs the read .csv file as a pandas DataFrame for use with other functions
    Errors:
    TypeError - if input is not a string
    ValueError - if input is not a valid table name

    """

    try:
        if len(table_name)==0:
            raise ValueError("No input name")
        
        #add csv extension
        file_name = table_name + ".csv" 

        #connect to the s3
        s3 = boto3.client('s3')

        #get the object with the input name
        file = s3.get_object(Bucket='ingested-data-vox-indicium', Key=file_name)

        #convert the body of the object in a DateFrame
        resulting_df = pd.read_csv(file['Body'])

        return resulting_df
    
    except ClientError as e:
        #catches the error if the user tap a non-existent table name
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise ValueError(f"The file {table_name} does not exist")
        
    except TypeError as e:
        #catches the error if the user tap an incorrect input 
        raise TypeError("Function must take a string input")
