import pandas as pd
import boto3


def fact_purchase_order_dataframe(address_table):

    try:
        # Check for empty input name
        if len(address_table) == 0:
            raise ValueError("No input name")

        # Define file name
        file_name = address_table + ".csv"

        # Connect to S3 client
        s3 = boto3.client('s3')

        file = s3.get_object(
            Bucket='ingestion-data-vox-indicium', Key=file_name)    

    fact_purchase_order = df_sales_order[[
    "sales_order_id", "created_at", "last_updated", "staff_id", "counterparty_id",
    "design_id", "units_sold", "unit_price", "currency_id",
    "agreed_delivery_date", "agreed_payment_date", "agreed_delivery_location_id"
    ]].copy()

    # Split date and time columns in the "created_at" and "last_updated" columns
    fact_purchase_order["created_date"] = fact_purchase_order["created_at"].dt.date
    fact_purchase_order["created_time"] = fact_purchase_order["created_at"].dt.time
    fact_purchase_order["last_updated_date"] = fact_purchase_order["last_updated"].dt.date
    fact_purchase_order["last_updated_time"] = fact_purchase_order["last_updated"].dt.time

    # Drop the original date and time columns
    fact_purchase_order.drop(["created_at", "last_updated"], axis=1, inplace=True)


    try:
        # Check for empty input name
        if len(address_table) == 0:
            raise ValueError("No input name")

        # Define file name
        file_name = address_table + ".csv"

        # Connect to S3 client
        s3 = boto3.client('s3')

        file = s3.get_object(
            Bucket='ingestion-data-vox-indicium', Key=file_name)

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
