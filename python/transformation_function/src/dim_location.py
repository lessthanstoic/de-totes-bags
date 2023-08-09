"""
This module reads .csv files from our ingestion bucket, and converts them
to a pandas data frame.
This module contains four functions:
dim_address_data_frame - reads the CSV file and returns a DataFrame.
create_and_push_parquet - converts the DataFrame to a parquet file and push
the parquet file in the process data bucket
main - runs all functions to create the final parquet file.
"""
import boto3
import pandas as pd
import io
from botocore.exceptions import ClientError


def dim_address_data_frame(address_table):
    """
    The function dim_address_data_frame reads a .csv file from our ingestion
    bucket and manipulates columns name with specific datatype and returns
    a nice data frame.
    Arguments:
    address_table (string) - represents the name of a table in our database.
    Output:
    resulting_df (DataFrame) - outputs the read .csv file as a pandas
    DataFrame for use with other functions
    Errors:
    TypeError - if input is not a string
    ValueError - Catching the specific ValueError
    ClientError - Catch the error if the table name is non-existent
    FileNotFoundError - if the file was not found
    Exception - for general errors
    """
    try:
        # Check for empty input name
        if len(address_table) == 0:
            raise ValueError("No input name")

        # Define file name
        file_name = address_table + ".csv"

        # Connect to S3 client
        s3 = boto3.client('s3')

        file = s3.get_object(
            Bucket='ingested-data-vox-indicium', Key=file_name)

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
        data_frame = pd.read_csv(io.StringIO(
            file['Body'].read().decode('utf-8')), names=col_names)

        # Drop the original datetime columns
        data_frame = data_frame.drop(columns=['created_at', 'last_updated'])

        # Change name of address_id to location_id
        data_frame.rename(columns={'address_id': 'location_id'}, inplace=True)

        # Set the column data types
        data_frame = data_frame.astype({
            "location_id": "int",
            "address_line_1": "str",
            "address_line_2": "str",
            "district": "str",
            "city": "str",
            "postal_code": "str",
            "country": "str",
            "phone": "str"
        })

        # Sorted the date frame
        data_frame.sort_values(by='location_id', inplace=True)

        # Return the final DataFrame
        return data_frame

    except ValueError as e:
        # Catching the specific ValueError before the generic Exception
        raise e

    except ClientError as e:
        # Catch the error if the table name is non-existent
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise ValueError(f"The file {address_table} does not exist")
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
