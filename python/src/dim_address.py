import boto3
import pandas as pd
import io
import os
from botocore.exceptions import (EndpointConnectionError, NoCredentialsError, ClientError)


def dim_address_data_frame(table_name):
    """
    The function dim_address_data_frame reads a .csv file from our ingestion bucket and manipulates columns name with specific datatype and returns a nice data frame.
    Arguments:
    table_name (string) - represents the name of a table in our database.
    Output:
    resulting_df (DataFrame) - outputs the read .csv file as a pandas DataFrame for use with other functions
    Errors:
    TypeError - if input is not a string
    ValueError - if input is not a valid table name
    """
    try:
        # Check for empty input name
        if len(table_name) == 0:
            raise ValueError("No input name")

        # Define file name
        file_name = table_name + ".csv"

        # Connect to S3 client
        s3 = boto3.client('s3')

        file = s3.get_object(Bucket='ingested-data-vox-indicium', Key=file_name)

        # Define the column names
        col_names = ["address_id",
                     "address_line_1",
                     "address_line_2",
                     "district",
                     "city",
                     "postal_code",
                     "country",
                     "phone",
                     "created_at",
                     "last_updated"
                     ]

        # Read the CSV file using the column names
        data_frame = pd.read_csv(io.StringIO(file['Body'].read().decode('utf-8')), names=col_names)

        # Convert the 'created_at' column to pandas datetime type
        data_frame['created_at'] = pd.to_datetime(data_frame['created_at'])

        # Extract date and time components from the 'created_at' column
        data_frame['created_date'] = data_frame['created_at'].dt.date
        data_frame['created_time'] = data_frame['created_at'].dt.time

        # Split the last update datetime into separate date and time updated columns
        data_frame['last_updated'] = pd.to_datetime(data_frame['last_updated'])
        data_frame['last_updated_date'] = data_frame['last_updated'].dt.date
        data_frame['last_updated_time'] = data_frame['last_updated'].dt.time

        # Drop the original datetime columns
        data_frame = data_frame.drop(columns=['created_at', 'last_updated'])

        # Set the column data types
        data_frame = data_frame.astype({
            "address_id": "int",
            "address_line_1": "str",
            "address_line_2": "str",
            "district": "str",
            "city": "str",
            "postal_code": "str",
            "country": "str",
            "phone": "str",
            "created_date": "str",
            "created_time": "str",
            "last_updated_date": "str",
            "last_updated_time": "str"
        })

        # Return the final DataFrame
        return data_frame

    except ValueError as e:
        # Catching the specific ValueError before the generic Exception
        raise e
    except ClientError as e:
        # Catch the error if the table name is non-existent
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise ValueError(f"The file {table_name} does not exist")
        else:
            raise e
    except TypeError as e:
        # catches the error if the user taps an incorrect input
        raise e
    except FileNotFoundError:
        raise FileNotFoundError(f"The file {file_name} does not exist")
    except Exception as e:
        # Generic exception to catch any other errors
        raise Exception(f"An unexpected error occurred: {e}")


def create_parquet(data_frame, table_name):
    """
    Convert the DataFrame to a parquet format.
    Arguments:
    data_frame - represent the DataFrame from the function dim_address_data_frame.
    table_name (string) - represents the name of a table in our database.
    """
    try:
        # Save DataFrame to a parquet file in memory
        parquet_buffer = io.BytesIO()
        data_frame.to_parquet(parquet_buffer, engine='pyarrow')

        s3 = boto3.client('s3')
        s3.put_object(Bucket='ingested-data-vox-indicium', Key=f'{table_name}.parquet', Body=parquet_buffer.getvalue())

        print(f"Parquet file '{table_name}.parquet' created in S3 bucket 'ingested-data-vox-indicium'.")

    except Exception as e:
        # Generic exception for unexpected errors during conversion
        raise Exception(f"An error occurred while converting to parquet: {e}")

def push_parquet_file(table_name):
    """
    Function to copy a Parquet file from one Amazon S3 bucket to another, then delete the original.
    param table_name: Name of the table (without extension) to be transferred.
    """
    try:
        s3 = boto3.client('s3')
        s3_resource = boto3.resource('s3')

        # Copy the parquet file
        copy_source = {
            'Bucket': 'ingested-data-vox-indicium',
            'Key': f'{table_name}.parquet'
        }

        s3_resource.meta.client.copy(copy_source, 'processed-data-vox-indicium', f'{table_name}.parquet')

        # Delete the original parquet file
        s3.delete_object(Bucket='ingested-data-vox-indicium', Key=f'{table_name}.parquet')

        print(f"Parquet file '{table_name}.parquet' transferred to S3 bucket 'processed-data-vox-indicium'.")
    except Exception as e:
        raise Exception(f"An error occurred while transferring the parquet file: {e}")

def main():
    """
    Runs both functions to create and transfer the final parquet file.
    """
    try:
        table_name = 'address'  
        df = dim_address_data_frame(table_name)  

        create_parquet(df, table_name)

        push_parquet_file(table_name)

    except Exception as e:
        print(f"An error occurred in the main function: {e}")

