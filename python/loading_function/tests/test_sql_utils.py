from python.loading_function.src.sql_utils import (
    copy_from_file,
    copy_from_stringio,
    get_table_primary_key)
from unittest import mock
import pandas as pd


@mock.patch('psycopg2.connect')
def test_insert_data_from_file(mock_connect):
    # ARRANGE
    # Mocking the database connection
    mock_conn = mock_connect.return_value
    # mock_cursor = mock_conn.cursor.return_value

    df = pd.read_csv("python/loading_function/tests/sales_order.csv")

    # ACT
    # Call the function being tested
    copy_from_file(mock_conn, df, "sales_order")

    # ASSERT
    # Some basic asserts we can make
    mock_conn.cursor.assert_called_once()
    mock_conn.commit.assert_called_once()


@mock.patch('psycopg2.connect')
def test_insert_data_from_stream(mock_connect):
    # ARRANGE
    # Mocking the database connection
    mock_conn = mock_connect.return_value
    # mock_cursor = mock_conn.cursor.return_value

    df = pd.read_csv("python/loading_function/tests/sales_order.csv")

    # ACT
    # Call the function being tested
    copy_from_stringio(mock_conn, df, "sales_order")

    # ASSERT
    # Some basic asserts we can make
    mock_conn.cursor.assert_called_once()
    mock_conn.commit.assert_called_once()


@mock.patch('psycopg2.connect')
def test_we_can_retrieve_primary_keys(mock_connect):
    # ARRANGE
    # Mocking the database connection
    mock_conn = mock_connect.return_value
    mock_cursor = mock_conn.cursor.return_value

    # Mock the result of the primary key query
    # List of tuples representing primary key columns
    primary_key_query_result = [('col1',), ('col2',)]

    # Mock the fetchall result for the query
    mock_cursor.fetchall.return_value = primary_key_query_result

    table_name = 'my_table'
    expected_primary_keys = ['col1', 'col2']

    # Assert the query execution
    expected_query = f"""
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid
            AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = '{table_name}'::regclass
        AND i.indisprimary;
    """

    # ACT
    primary_keys = get_table_primary_key(mock_conn, table_name)

    # ASSERT
    mock_cursor.execute.assert_called_once_with(expected_query)
    mock_conn.cursor.assert_called_once()
    mock_cursor.fetchall.assert_called_once()
    assert primary_keys == expected_primary_keys


# Alas I cannot find a way to mock test this functionality
# @mock.patch('psycopg2.connect')
# def test_update_warehouse_from_file(mock_connect):
#     # ARRANGE
#     # Mocking the database connection
#     mock_conn = mock_connect.return_value
#     mock_cursor = mock_conn.cursor.return_value
#     mock_execute_values = mock.MagicMock()
#     mock_cursor.__enter__.return_value = mock_execute_values

#     data = {
#         'Column1': [10, 20],
#         'Column2': ['A', 'B']
#     }
#     df = pd.DataFrame(data)

#     # columns = df.columns.tolist()
#     # column_names = ', '.join(columns)
#     # primary_keys = ['sales_order_id']
#     # primary_keys_string = ', '.join(primary_keys)
#     # values = """((10, 'A'), (20, 'B))"""
#     # primary_keys_excluded = ["EXCLUDED." + x.strip()
#     #                          for x in primary_keys]
#     # excluded = ', '.join(primary_keys_excluded)

#     # merge_query = f"""
#     #     INSERT INTO sales_order ({column_names})
#     #     VALUES ({values})
#     #     ON CONFLICT ({primary_keys_string})
#     #     DO UPDATE SET ({primary_keys_string}) = ({excluded});
#     # """
#     # print( merge_query )
#     # ACT
#     # Call the function being tested
#     update_from_file(mock_conn, df, "sales_order", ['Column1'])

#     # ASSERT
#     # Some basic asserts we can make
#     mock_conn.cursor.assert_called_once()
#     # mock_conn.commit.assert_called_once()


# Alas I cannot find a way to mock test this functionality
# @mock.patch('psycopg2.connect')
# def test_update_warehouse_from_file_single_row(mock_connect):
#     # ARRANGE
#     # Mocking the database connection
#     mock_conn = mock_connect.return_value
#     mock_cursor = mock_conn.cursor.return_value

#     data = {
#         'Column1': [10],
#         'Column2': ['A']
#     }
#     df = pd.DataFrame(data)

#     # ACT
#     # Call the function being tested
#     update_from_file(mock_conn, df, "sales_order", ['Column1'])

#     # ASSERT
#     # Some basic asserts we can make
#     mock_conn.cursor.assert_called_once()
#     # mock_conn.commit.assert_called_once()
