import boto3
import pandas as pd
import io
from botocore.exceptions import ClientError


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


def create_and_push_parquet(data_frame, table_name):
    '''
    Convert the DataFrames to a parquet format and push it to the processed s3 bucket.
    Arguments:
    data_frame - represent the DataFrame from of address table.
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
        table_name = 'address'  
        dim_location = "dim_location"
        df = dim_address_data_frame(table_name)  

        create_and_push_parquet(df, dim_location)

    except Exception as e:
        print(f"An error occurred in the main function: {e}")

