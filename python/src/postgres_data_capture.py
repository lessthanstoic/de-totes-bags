from src.secret_login import retrieve_secret_details
from src.parameter_store import (
    save_to_parameter_store,
    load_from_parameter_store)
import psycopg2
import re


ENDPOINT = '''nc-data-eng-totesys-production.
chpsczt8h1nu.eu-west-2.rds.amazonaws.com'''
PORT = 5432
DBNAME = "totesys"


def postgres_data_capture():

    param_store = "postgres-datetime"
    try:
        old_datetime = load_from_parameter_store(param_store)
    except Exception as e:
        # log the error
        raise e  # want to stop the program right now

    try:
        db_login_deets = retrieve_secret_details("Totes-Login-Credentials")
    except Exception as e:
        # log the error
        raise e  # want to stop the program right now

    try:
        connection = psycopg2.connect(
            host=ENDPOINT,
            port=PORT,
            database=DBNAME,
            user=db_login_deets['username'],
            password=db_login_deets['password']
        )
        # log the connection
        # print("Connected to the database!")
    except psycopg2.Error as e:
        # log the error
        # print(f"Error: Unable to connect to the database - {e}")
        raise e

    # Create a cursor object to execute SQL queries
    cursor = connection.cursor()

    # Get current datetime
    datetime_query = "SELECT NOW();"
    cursor.execute(datetime_query)
    current_datetime = cursor.fetchall()

    # SQL query to fetch all records from a table
    query = '''SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = "public";'''
    # Execute the query
    cursor.execute(query)

    # Fetch all records
    table_list = cursor.fetchall()

    for table_items in table_list:
        string_item = str(table_items)
        # replace "(" or ")" with ""
        string_item = re.sub(r"[,()']", "", string_item)

        table_query = f'''SELECT *
        FROM {string_item}
        WHERE created_at
        BETWEEN {old_datetime} AND {current_datetime}
        OR last_updated
        BETWEEN {old_datetime} AND {current_datetime};'''
        table_query = re.sub(r"[,()']", "", table_query)

        cursor.execute(table_query)
        table = cursor.fetchall()
        print(table)

        # write_csv_to_local(table)
        # transfer_csv_to_s3(table)
        # call function write table as csv to s3 bucket

    # Close connections
    cursor.close()
    connection.close()

    try:
        save_to_parameter_store(param_store, current_datetime)
    except Exception as e:
        # log the error
        raise e  # want to stop the program right now


postgres_data_capture()