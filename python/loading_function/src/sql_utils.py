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
# for the copy_from_file and copy_from_stringio functions


def copy_from_file(conn, df, table):
    """
    Here we are going save the dataframe on disk as
    a csv file, load the csv file
    and use copy_from() to copy it to the table
    """
    # Save the dataframe to disk
    tmp_df = "./tmp_dataframe.csv"
    df.to_csv(tmp_df, index=False, header=False)

    f = open(tmp_df, 'r')
    cursor = conn.cursor()
    try:
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
    Here we are going save the dataframe in memory
    and use copy_from() to copy it to the table
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
