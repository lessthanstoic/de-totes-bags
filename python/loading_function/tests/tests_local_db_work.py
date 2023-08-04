# flake8: noqa
import pandas as pd 
import io 
import psycopg2
import psycopg2 as extras 
import fastparquet 
from python.loading_function.src.sql_utils import (copy_from_file, get_table_primary_key, update_from_file)


# df = pd.read_parquet("python/loading_function/tests/sales_order_changes.parquet", engine='fastparquet')
# df.head().to_csv("python/loading_function/tests/sales_order_updates.csv", index=False, header=True)
# columns = df.columns.tolist()
# column_names = ', '.join(columns)
# print(df.head())


def test_postgres_connection_and_creates_table():

    conn_string = "host='localhost' dbname='postgres' user='amy' password='password'"
    conn = psycopg2.connect(conn_string)

    print("Database opened successfully")

    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE test_table(
                   sales_record_id SERIAL PRIMARY KEY,
                   sales_order_id INT,
                   created_date DATE,
                   created_time TIME,
                   last_updated_date DATE,
                   last_updated_time TIME,
                   sales_staff_id INT,
                   counterparty_id INT,
                   units_sold INT,
                   unit_price NUMERIC(10, 2),
                   currency_id INT,
                   design_id INT,
                   agreed_payment_date DATE,
                   agreed_delivery_date DATE,
                   agreed_delivery_location_id INT);''')
    
    conn.commit()
    conn.close()
    print("Database Closed successfully")


def test_inserts_data():

    conn_string = "host='localhost' dbname='postgres' user='amy' password='password'"
    conn = psycopg2.connect(conn_string)

    print("Database opened successfully")
    copy_from_file(conn, df, 'test_table')
    
    conn.commit()
    conn.close()


def test_updates_data():

    df = pd.read_csv("python/loading_function/tests/sales_order_updates.csv")
    print(df.head())
    conn_string = """host='localhost' dbname='postgres'
    user='amy' password='password'"""
    conn = psycopg2.connect(conn_string)

    pk = get_table_primary_key(conn, 'test_table')
    update_from_file(conn, df, 'test_table', pk)

    conn.commit()


# test_postgres_connection_and_creates_table()
# test_inserts_data()
# test_updates_data()

