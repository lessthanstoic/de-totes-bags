"""
This module reads .csv files from our ingestion bucket, and converts them to a pandas data frame.
This module contains one function:
sales_order_data_frame
create_parquet
main
"""
import boto3
import pandas as pd
from botocore.exceptions import ClientError
from pprint import pprint
from moto import mock_s3
import io
import os


@mock_s3
def sales_order_data_frame(table_name):
    """
    The function sales_order_data_frame reads a .csv file from our ingestion bucket and manipulate columns name with specific datatype and return a nice data frame.
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
        file_name = table_name + ".csv"
        s3 = boto3.client('s3')

        #this line need to be delete 
        s3.create_bucket(Bucket='ingested-data-vox-indicium', CreateBucketConfiguration={
           'LocationConstraint': 'eu-west-2',
        })
        
        with open('python/src/csv_files/sales_order.csv', 'rb') as data:
            s3.put_object(Bucket='ingested-data-vox-indicium', Body=data, Key='sales_order.csv')
        #until 

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

        # Split the time columns into separate time columns without milliseconds
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
        
        return data_frame
    
    except ClientError as e:
        #catches the error if the user tap a non-existent table name
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise ValueError(f"The file {table_name} does not exist")
        else:
            raise
    except TypeError as e:
       #catches the error if the user tap an incorrect input
        raise e
    
def create_parquet(data_frame, table_name):
    """
    The function create_parquet convert the data frame in a parquet format.
    Arguments:
    table_name (string) - represents the name of a table in our database.
    data_frame - represent the data frame from function sales_order_data_frame.
    """

    # Save DataFrame to a parquet file
    data_frame.to_parquet(f'{table_name}.parquet', engine='pyarrow')

def main():
    """
    The function main run both function to create the final parquet file.    
    Output:
    parquet file
    """

    # Retrieve the data from the S3 bucket
    table_name = 'sales_order'
    df = sales_order_data_frame(table_name)
    parquet_directory = 'parquet_file'

    #need to do something to create this files in a directory 
    parquet_directory = "parquet_files"
    if not os.path.exists(parquet_directory):
        os.makedirs(parquet_directory)
    file_path = os.path.join(parquet_directory, f'{table_name}')
       
    print(f"Parquet file '{file_path}' created successfully.")

    create_parquet(df, file_path)

    #need to make a function to push this parquet files in the process s3 buck
    
main()

