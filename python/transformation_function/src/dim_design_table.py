"""
This module reads .csv files from our ingestion bucket, and converts them to a pandas data frame.
This module contains four functions:
dim_design_table_data_frame- reads the CSV file and returns a DataFrame.
create_and_push_parquet - converts the DataFrame to a parquet file and push the parquet file in the process data bucket
main - runs all functions to create the final parquet file.
"""
import boto3
import pandas as pd
import io
from botocore.exceptions import ClientError

def dim_design_table_data_frame(design_table):
    """
    The function design_table_data_frame reads a .csv file from our ingestion bucket and manipulate columns name with specific datatype and return a nice data frame.
    Arguments:
    design_table (string) - represents the name of a table in our database.
    Output:
    resulting_df (DataFrame) - outputs the read .csv file as a pandas DataFrame for use with other functions
    Errors:
    TypeError - if input is not a string
    ValueError - Catching the specific ValueError
    ClientError - Catch the error if the table name is non-existent
    FileNotFoundError - if the file was not found
    Exception - for general errors
    """

    try:
        # Check for empty input name
        if len(design_table)==0:
            raise ValueError("No input name")
        
        # Define file name
        file_name = design_table + ".csv"
        
        # Connect to S3 client
        s3 = boto3.client('s3')

        # Get the objects from ingested-data-vox-indicium S3 bucket
        file = s3.get_object(Bucket='ingested-data-vox-indicium', Key=file_name)

        # # Define the column names
        # col_names = ['design_id',
        #             'created_at',
        #             'design_name',
        #             'file_location',
        #             'file_name',
        #             'last_updated'
        #             ]

        
        # Read the CSV file using the column names
        data_frame = pd.read_csv(io.StringIO(file['Body'].read().decode('utf-8')))

        # Drop the original datetime columns
        data_frame = data_frame.drop(columns=['created_at', 'last_updated'])
        
        # Set the column data types
        data_frame = data_frame.astype({
            'design_id': 'int',
            'design_name': 'str',
            'file_location': 'str',
            'file_name': 'str'
            })
        
        # Sorted the date frame
        data_frame.sort_values(by='design_id', inplace=True)
        
        # Return the final DataFrame
        return data_frame
    
    except ValueError as e:
        # Catching the specific ValueError before the generic Exception
        raise e
    
    except ClientError as e:
        # Catch the error if the table name is non-existent
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise ValueError(f"The file {design_table} does not exist")
        else:
            raise e
        
    except TypeError as e:
       #catches the error if the user tap an incorrect input
        raise e
    
    except FileNotFoundError:
        raise FileNotFoundError(f"The file {file_name} does not exist")
    
    except Exception as e:
        # Generic exception to catch any other errors
        raise Exception(f"An unexpected error occurred: {e}")

def create_and_push_parquet(data_frame, new_table):
    '''
    Convert the DataFrames to a parquet format and push it to the processed s3 bucket.
    Arguments:
    data_frame - represent the DataFrame from of sales table the function fact_sales_order_data_frame.
    new_table(string) - represents the name of the final table from processed-data-vox-indicium s3 bucket.
    '''
    try:
       # Save DataFrame to a parquet file in memory
        parquet_buffer = io.BytesIO()
        data_frame.to_parquet(parquet_buffer, engine='pyarrow')

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
    """
    Runs all functions to create and transfer the final parquet file.
    """
    try:
        # Table name for the tables used in the function dim_design_table_data_frame
        design_table = 'design'

        # The name of the parquet file
        new_table = "dim_design"

        # Call the design_table_data_frame function
        df = dim_design_table_data_frame(design_table)

        #Call the create_and_push_parquet function
        create_and_push_parquet(df, new_table)

    # Generic exception for unexpected errors during the running of the functions
    except Exception as e:
        print(f"An error occurred in the main function: {e}")
