"""
This module reads .csv files from our ingestion bucket, and converts them to a pandas data frame.
This module contains three functions:
dim_currency_data_frame - reads the CSV file and returns a DataFrame.
create_parquet - converts the DataFrame to a parquet file.
push_parquet_file - copies the parquet file from one Amazon S3 bucket to another and then deletes the original.
main - runs both functions to create and transfer the final parquet file.
"""
import boto3
import pandas as pd
import io
from botocore.exceptions import ClientError

def dim_currency_data_frame(table_name):
    """
    The function dim_currency_data_frame reads a .csv file from our ingestion bucket and manipulates column names with specific data types, then returns a nice DataFrame.
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
        col_names = ["currency_id",
                     "currency_code",
                     "created_at",
                     "last_updated"
                     ]

        # Read the CSV file using the column names
        data_frame = pd.read_csv(io.StringIO(file['Body'].read().decode('utf-8')), names=col_names)

        data_frame = data_frame.drop(columns=['created_at', 'last_updated'])

        var_currency = {'GBP' : "Pound",
                        'USD': 'Dollar',
                        'EUR': "Euro"}
        
        data_frame['currency_name'] = data_frame['currency_code'].map(var_currency)

        # Set the column data types
        data_frame = data_frame.astype({
            "currency_id": "int",
            "currency_code": "str",
            "currency_name": "str"
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
        # Catches the error if the user taps an incorrect input
        raise e
    except FileNotFoundError:
        raise FileNotFoundError(f"The file {file_name} does not exist")
    except Exception as e:
        # Generic exception to catch any other errors
        raise Exception(f"An unexpected error occurred: {e}")

def create_and_push_parquet(data_frame, table_name):
    '''
    Convert the DataFrames to a parquet format and push it to the processed s3 bucket.
    Arguments:
    data_frame - represent the DataFrame from of sales table the function dim_currency_data_frame.
    table_name(string) - represents the name of a table in our database.
    '''
    try:
       # Save DataFrame to a parquet file in memory
        parquet_buffer = io.BytesIO()
        data_frame.to_parquet(parquet_buffer, engine='pyarrow')

        s3 = boto3.client('s3')
        s3.put_object(Bucket='processed-data-vox-indicium', Key=f'{table_name}.parquet', Body=parquet_buffer.getvalue())
       
        print(f"Parquet file '{table_name}.parquet' created and stored in S3 bucket 'processed-data-vox-indicium'.")
        
    except Exception as e:
        # Generic exception for unexpected errors during conversion
        raise Exception(f"An error occurred while converting to parquet: {e}")

def main():
    """
    Runs both functions to create and transfer the final parquet file.
    """
    try:
        table_name = 'currency'
        dim_currency = "dim_currency"
        df = dim_currency_data_frame(table_name)

        create_and_push_parquet(df, dim_currency)

    except Exception as e:
        print(f"An error occurred in the main function: {e}")

print(main())