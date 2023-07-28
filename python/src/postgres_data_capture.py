from src.secret_login import retrieve_secret_details
from src.s3_timestamp import get_s3_timestamp
from src.csv_write import write_table_to_csv
from src.push_data_in_bucket import push_data_in_bucket
import psycopg2
import re


# To maybe place in a secret later
ENDPOINT = '''nc-data-eng-totesys-production.
chpsczt8h1nu.eu-west-2.rds.amazonaws.com'''
PORT = 5432
DBNAME = "totesys"


def postgres_data_capture(event, context):

    param_store = "postgres-datetime.txt"
    try:
        old_datetime = get_s3_timestamp(param_store)
    except Exception as e:
        # log the error
        raise e  # want to stop the program right now

    try:
        db_login_deets = retrieve_secret_details("Totesys-Access")
    except Exception as e:
        # log the error
        raise e  # want to stop the program right now

    try:
        connection = psycopg2.connect(
            host=db_login_deets['host'],
            port=int(db_login_deets['port']),
            database=db_login_deets['database'],
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
        table_name = str(table_items)
        # replace "(" or ")" with ""
        table_name = re.sub(r"[,()']", "", table_name)

        table_query = f'''SELECT *
        FROM {table_name}
        WHERE created_at
        BETWEEN {old_datetime} AND {current_datetime}
        OR last_updated
        BETWEEN {old_datetime} AND {current_datetime};'''
        table_query = re.sub(r"[,()']", "", table_query)

        full_table_query = f'''SELECT *
        FROM {table_name};'''
        full_table_query = re.sub(r"[,()']", "", full_table_query)

        cursor.execute(table_query)
        table_changes = cursor.fetchall()

        write_table_to_csv(table_changes, f'{table_name}_changes')
        push_data_in_bucket('./csv_files/', f'{table_name}_changes.csv')

        cursor.execute(full_table_query)
        full_table = cursor.fetchall()

        write_table_to_csv(full_table, table_name)
        push_data_in_bucket('./csv_files/', f'{table_name}.csv')

    # Close connections
    cursor.close()
    connection.close()

    with open(param_store, 'w') as f:
        f.write(current_datetime)

    try:
        push_data_in_bucket("./", param_store)

    except Exception as e:
        # log the error
        raise e  # want to stop the program right now
