import psycopg2

def update_from_file(conn, df, table):

    cursor = conn.cursor()
    update_query = f"""UPDATE {table} AS t
                  SET name = e.name 
                  FROM (VALUES %s) AS e(name, id) 
                  WHERE e.id = t.id;"""

    psycopg2.extras.execute_values 
    (cursor, update_query, new_values, template=None, page_size=100 )

    primary_keys = get_table_primary_key(conn, table)
    primary_keys = ', '.join(primary_keys) (edited) 


def get_table_primary_key(conn, table):
    # https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
    query = f"""
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = '{table}'::regclass
        AND i.indisprimary;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        query_results = cursor.fetchall()
    primary_keys = [x for x in query_results]
    return primary_keys

    # Convert the DataFrame into a list of tuples
    data_tuples = [tuple(row) for row in df.to_numpy()]
    # get the primary keys for our table via sql query
    # surely this should be stored in parquet meta
    primary_keys_list = get_table_primary_key(conn, table)
    primary_keys = ', '.join(primary_keys_list)
    columns = df.columns.tolist()
    column_names = ', '.join(columns)
    value_placeholders = ', '.join(['%s'] * len(columns))
    primary_keys_excluded = ["EXCLUDED." + x.strip() for x in primary_keys_list]
    excluded = ', '.join(primary_keys_excluded)
    # Define the SQL query for INSERT or UPDATE
    merge_query = f"""
        INSERT INTO {table} ({column_names})
        VALUES ({value_placeholders})
        ON CONFLICT ({primary_keys})
        DO UPDATE SET ({primary_keys}) = ({excluded});
    """
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, merge_query, data_tuples)
        conn.commit()
    except(Exception, psycopg2.DatabaseError) as e:
        print("Error: %s" % e)
        conn.rollback()
        cursor.close()
        return 1
    # print("the dataframe is inserted") # log this instead
    cursor.close()