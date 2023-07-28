'''
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
'''

#New Version updated 

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from pprint import pprint
from moto import mock_s3
import io


@mock_s3
def retrieve_previous_data(table_name, col_names):

    try:
        if len(table_name)==0:
            raise ValueError("No input name")
        file_name = table_name + ".csv"
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket='ingested-data-vox-indicium', CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-2',
        })
        with open('example.csv', 'rb') as data:
            s3.put_object(Bucket='ingested-data-vox-indicium', Body=data, Key='example.csv')
        #s3 = boto3.client('s3')
       # s3.create_bucket(Bucket='ingested-data-vox-indicium', CreateBucketConfiguration={
        #'LocationConstraint': 'eu-west-2'})

        file = s3.get_object(Bucket='ingested-data-vox-indicium', Key=file_name)
        # Define the column names
        col_names = ['sales_order_id', 'created_datetime',
                     'last_updated_datetime','design_id', 'sales_staff_id', 'counterparty_id',
                     'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date','agreed_payment_date',
                      'agreed_delivery_location_id']
        
        # Use the column names when reading the CSV
        data_frame = pd.read_csv(io.StringIO(file['Body'].read().decode('utf-8')), names=col_names)

        # Split the datetime columns into separate date and time columns
        data_frame[['created_date', 'time']] = data_frame['created_datetime'].str.split(' ', expand=True)
        data_frame[['created_time', 't']] = data_frame['time'].str.split('.', expand=True)
        data_frame[['last_updated_date', 'ltime']] = data_frame['last_updated_datetime'].str.split(' ', expand=True)
        data_frame[['last_updated_time', 'lt']] = data_frame['ltime'].str.split('.', expand=True)
        # Drop the original datetime columns
        data_frame = data_frame.drop(columns=['created_datetime', 'last_updated_datetime', 'time', 'ltime', 't', 'lt'])
        # Set the column data types
        data_frame = data_frame.astype({
            'sales_order_id': 'int',
            'created_date': 'str',  
            'created_time': 'str',
            'last_updated_date': 'str',
            'last_updated_time': 'str',
            'design_id': 'int',
            'sales_staff_id': 'int',
            'counterparty_id': 'int',
            'units_sold': 'int',
            'unit_price': 'float',
            'currency_id': 'int',
            'agreed_delivery_date': 'str',
            'agreed_payment_date': 'str',
            'agreed_delivery_location_id': 'int'
        })
        #datetime64[ns]
        print(data_frame)
        return data_frame
    except ClientError as e:
        #catches the error if the user tap a non-existent table name
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise ValueError(f"The file {table_name} does not exist")
        else:
            raise
    except TypeError as e:
       #catches the error if the user tap an incorrect input
        raise TypeError("Function must take a string input")
    
def create_parquet(data_frame, table_name):
    # Save DataFrame to a parquet file
    data_frame.to_parquet(f'{table_name}.parquet', engine='pyarrow')

def main():
    # Retrieve the data from the S3 bucket
    table_name = 'example'
    df = retrieve_previous_data(table_name)
    create_parquet(df, table_name)


pprint(main())
