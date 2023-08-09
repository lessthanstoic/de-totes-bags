"""
This script provides functions to interact with a PostgreSQL database
for data loading and updating.

The primary purpose of this script is to provide utility functions to
efficiently copy data from pandas DataFrames to PostgreSQL tables using
the COPY command and perform upsert (INSERT ON CONFLICT) operations
for data updates. The script includes functions for copying data from both
file and in-memory StringIO buffer. Additionally, the script includes a
function to retrieve primary key columns of a PostgreSQL table.

Functions:
- copy_from_file: Copy data from a DataFrame to a PostgreSQL table using
the COPY command.
- copy_from_stringio: Copy data from a DataFrame to a PostgreSQL table using
the COPY command and StringIO.
- update_from_file: Update data in a PostgreSQL table from a DataFrame using
an upsert operation.
- get_table_primary_key: Retrieve primary key columns of a PostgreSQL table.

Usage:
1. Ensure the necessary libraries (psycopg2, io, logging) are available.
2. Import the required utility functions from this script.
3. Use the provided functions to copy data to PostgreSQL tables and
perform updates.

Note:
- The functions provided in this script assume proper configuration and
connectivity to a PostgreSQL database.
- Error handling is provided to catch and log database-related exceptions.
"""
import os
import psycopg2
import psycopg2.extras as extras
from io import StringIO
import logging

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

# With thanks to
# https://naysan.ca/2020/05/09/
# pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/
# for the copy_from_file and copy_from_stringio functions which
# were changed to fit our needs


def copy_from_file(conn, df, table):
    """
    Copy data from a DataFrame to a PostgreSQL table using the COPY command.

    This function saves the DataFrame to a temporary CSV file,
    then uses the psycopg2 `copy_from` method to copy the data
    from the CSV file into the specified PostgreSQL table.

    Args:
        conn: The PostgreSQL database connection.
        df (pandas.DataFrame): The DataFrame containing data to be copied.
        table (str): The name of the PostgreSQL table to copy the data into.

    Returns:
        None

    Raises:
        psycopg2.DatabaseError: If there's an error during the
        database operation.
    """
    try:
        tmp_df = "/tmp/tmp_dataframe.csv"
        df.to_csv(tmp_df, index=False, header=False)

        f = open(tmp_df, 'r')
        cursor = conn.cursor()
        # Thinking ahead of time:
        # if we have quotes and comma's together
        # ie. ",Madrid,SN,,SEN,,,SN,173,157"
        # the we may need to do similar to the below to make it work:
        # curs.copy_expert("""COPY mytable
        # FROM STDIN WITH (FORMAT CSV)""", _io_buffer)
        cursor.copy_from(f, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        os.remove(tmp_df)
        logger.error("Error: database error", error)
        conn.rollback()
        cursor.close()
        return 1
    logger.info("copy_from_file() done")
    cursor.close()
    os.remove(tmp_df)


def copy_from_stringio(conn, df, table):
    """
    Copy data from a DataFrame to a PostgreSQL table using the
    COPY command and StringIO.

    This function saves the DataFrame to an in-memory StringIO buffer,
    then uses the psycopg2 `copy_from` method to copy the data from
    the buffer into the specified PostgreSQL table.

    Args:
        conn: The PostgreSQL database connection.
        df (pandas.DataFrame): The DataFrame containing data to be copied.
        table (str): The name of the PostgreSQL table to copy the data into.

    Returns:
        None

    Raises:
        psycopg2.DatabaseError: If there's an error during the
        database operation.
    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, index_label='id', header=False)
    buffer.seek(0)

    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error("Error: database error", error)
        conn.rollback()
        cursor.close()
        return 1
    logger.info("copy_from_stringio() done")
    cursor.close()


def update_from_file(conn, df, table, primary_keys_list):
    """
    Update data in a PostgreSQL table from a DataFrame.

    This function updates data in a PostgreSQL table using an
    upsert (INSERT ON CONFLICT) operation. It takes a DataFrame,
    converts it into a list of tuples, and performs an upsert
    using the specified primary key columns.

    Args:
        conn: The PostgreSQL database connection.
        df (pandas.DataFrame): The DataFrame containing data to be updated.
        table (str): The name of the PostgreSQL table to update.
        primary_keys_list (list): List of primary key column names.

    Returns:
        None

    Raises:
        psycopg2.DatabaseError: If there's an error during the
        database operation.
    """
    # Convert the DataFrame into a list of tuples
    # This is essentially a tuple per row of data we wish to insert
    if len(df.index) == 0:
        logger.info("No data to copy")
        return
    elif len(df.index) == 1:
        data_tuples = [tuple(df.to_numpy()[0])]
    else:
        data_tuples = [tuple(row) for row in df.to_numpy()]
    # get the primary keys for our table via sql query
    # surely this should be stored in parquet meta?
    # primary_keys_list = get_table_primary_key(conn, table)
    primary_keys = ', '.join(primary_keys_list)
    columns = df.columns.tolist()
    update_columns = [x for x in columns if x not in primary_keys_list]
    column_names = ', '.join(columns)
    update_columns_names = ", ".join(update_columns)
    primary_keys_excluded = ["EXCLUDED." + x.strip()
                             for x in update_columns]
    excluded = ', '.join(primary_keys_excluded)
    merge_query = f"""
        INSERT INTO {table} ({column_names})
        VALUES %s
        ON CONFLICT ({primary_keys})
        DO UPDATE SET ({update_columns_names}) = ({excluded});
    """
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, merge_query, data_tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error("Error: database error")
        conn.rollback()
        cursor.close()
        raise e
    logger.info(f"{table} has been updated")
    cursor.close()


def get_table_primary_key(conn, table):
    """
    Retrieve primary key columns of a PostgreSQL table.

    This function retrieves the primary key columns of a
    PostgreSQL table using a SQL query.

    Args:
        conn: The PostgreSQL database connection.
        table (str): The name of the PostgreSQL table.

    Returns:
        list: A list of primary key column names.

    Raises:
        psycopg2.DatabaseError: If there's an error during
        the database operation.
    """
    # https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
    query = f"""
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid
            AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = '{table}'::regclass
        AND i.indisprimary;
    """
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        query_results = cursor.fetchall()
        primary_keys = [row[0] for row in query_results]
    except (Exception, psycopg2.DatabaseError):
        logger.error("Error: database error")
        conn.rollback()
        cursor.close()
        return 1

    logger.info("Primary Keys retrieved from database")
    return primary_keys
