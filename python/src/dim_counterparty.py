"""
This module reads .csv files from our ingestion bucket, and converts them to a pandas data frame.
This module contains four functions:
dim_counterparty_data_frame - reads the CSV files and returns a DataFrame.
create_parquet - converts the DataFrame to a parquet file.
push_parquet_file - push the parquet file in the process data bucket
main - runs all functions to create the final parquet file.
"""

import boto3
import pandas as pd
import io
from botocore.exceptions import  ClientError

def dim_counterparty_data_frame(counterparty_table, address_table):
    """
    The function dim_counterparty_data_frame reads the .csv files from our ingestion bucket and manipulate columns name with specific datatype and return a nice data frame.
    Arguments:
    counterparty_table (string) - represents the name of counterparty table from ingestion bucket.
    address_table (string) - represents the name of address table from ingestion bucket.
    Output:
    data_frame (DataFrame) - outputs the read .csv files as a pandas DataFrame with information from the both tables
    Errors:
    TypeError - if input is not a string
    ValueError - Catching the specific ValueError
    ClientError - Catch the error if the table name is non-existent
    FileNotFoundError - if the file was not found
    Exception - for general errors 

    """
    
    try:
        # Check for empty input name
        if len(counterparty_table)==0 or len(address_table)==0:
            raise ValueError("No input name")
        
        # Define file name
        counterparty_name = counterparty_table + ".csv"
        address_name = address_table + ".csv"
        
        # Connect to S3 client
        s3 = boto3.client('s3')

        # Get the objects from ingested-data-vox-indicium S3 bucket
        counterparty_file = s3.get_object(Bucket='ingested-data-vox-indicium', Key=counterparty_name)
        address_file = s3.get_object(Bucket='ingested-data-vox-indicium', Key=address_name)

        # Define the column names
        counterparty_col_names = ["counterparty_id", 
                                "counterparty_legal_name",
                                "legal_address_id",
                                "commercial_contact",
                                "delivery_contact",
                                "created_at",
                                "last_updated"
                                ]
        
        address_col_names = ['address_id',
                            'address_line_1', 
                            'address_line_2', 
                            'district', 
                            'city', 
                            'postal_code', 
                            'country', 
                            'phone', 
                            'created_at', 
                            'last_updated' 
                            ]
        
        # Read the CSV files using the column names
        counterparty_df = pd.read_csv(io.StringIO(counterparty_file['Body'].read().decode('utf-8')), names=counterparty_col_names, header=None)
        address_df = pd.read_csv(io.StringIO(address_file['Body'].read().decode('utf-8')), names=address_col_names, header=None)

        # Merge counterparty_df and address_df DataFrames on matching 'legal_address_id' and 'address_id', retaining distinct suffixes for overlapping columns
        merged_df = pd.merge(counterparty_df, address_df, left_on='legal_address_id', right_on='address_id', suffixes=('', '_address'))

        # Rename and reorder the columns 
        data_frame = merged_df.rename(columns={
            'counterparty_legal_name': 'counterparty_legal_name',
            'address_line_1': 'counterparty_legal_address_line_1',
            'address_line_2': 'counterparty_legal_address_line2',
            'district': 'counterparty_legal_district',
            'city': 'counterparty_legal_city',
            'postal_code': 'counterparty_legal_postal_code',
            'country': 'counterparty_legal_country',
            'phone': 'counterparty_legal_phone_number'
        })

        selected_columns = [
            'counterparty_id', 'counterparty_legal_name',
            'counterparty_legal_address_line_1', 'counterparty_legal_address_line2',
            'counterparty_legal_district', 'counterparty_legal_city',
            'counterparty_legal_postal_code', 'counterparty_legal_country',
            'counterparty_legal_phone_number'
        ]

        # Create the date frame with the desired columns
        data_frame = data_frame[selected_columns]

        # Set the column data types for the final table
        data_frame = data_frame.astype({
            "counterparty_id": "int",
            "counterparty_legal_name": "str",
            "counterparty_legal_address_line_1": "str",
            "counterparty_legal_address_line2": "str",
            "counterparty_legal_district": "str",
            "counterparty_legal_city": "str",
            "counterparty_legal_postal_code": "str",
            "counterparty_legal_country": "str",
            "counterparty_legal_phone_number": "str"
        })
        
        # Return the final DataFrame
        return data_frame
    
    except ValueError as e:
        # Catching the specific ValueError before the generic Exception
        raise e
    
    except ClientError as e:
        # Catch the error if the table name is non-existent
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise ValueError(f"The file {counterparty_table} or {address_table} does not exist")
        else:
            raise e
        
    except TypeError as e:
        # Catches the error if the user tap an incorrect input
        raise e
    
    except FileNotFoundError:
        raise FileNotFoundError(f"The file {counterparty_table} or {address_table} does not exist")
    
    except Exception as e:
        # Generic exception to catch any other errors
        raise Exception(f"An unexpected error occurred: {e}")

def create_and_push_parquet(df, new_table):
    '''
    Convert the DataFrames to a parquet format and push it to the processed-data-vox-indicium s3 bucket.
    Arguments:
    df - represents the data frame resulting from combining the counterparty and address tables in the function dim_counterparty_data_frame 
    new_table(string) - represents the name of the final table from processed-data-vox-indicium s3 bucket.
    '''
    try:
        # Save DataFrame to a parquet file in memory
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow')

        # Connect to S3 client
        s3 = boto3.client('s3')

        # Send the parquet file to processed-data-vox-indicium s3 bouquet
        s3.put_object(Bucket='processed-data-vox-indicium', Key=f'{new_table}.parquet', Body=parquet_buffer.getvalue())
       
        # Print a confirmation message
        print(f"Parquet file '{new_table}.parquet' created and stored in S3 bucket 'processed-data-vox-indicium'.")
        
    except Exception as e:
        # Generic exception for unexpected errors during conversion
        raise Exception(f"An error occurred while converting to parquet: {e}")
    
def main():
    '''
    Runs both functions to create and transfer the final parquet file.
    '''
    try:
        # Tables names for the tables used in the function dim_counterparty_data_frame
        counterparty_table = 'counterparty'
        address_table = 'address'

        # The name of the parquet file
        new_table = "dim_counterparty"

        # Call the dim_counterparty_data_frame function
        df = dim_counterparty_data_frame(counterparty_table, address_table)

        #Call the create_and_push_parquet function
        create_and_push_parquet(df, new_table)

    # Generic exception for unexpected errors during the running of the functions
    except Exception as e:
        print(f"An error occurred in the main function: {e}")

