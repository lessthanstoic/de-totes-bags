from python.src.csv_write import write_table_to_csv
import os


def test_writes_a_table_csv():
    table_query = [["user_id", "name", "password"],
                   ["1", "Andrei", "Password123"],
                   ["2", "Michael", "Liverpool654"],
                   ["3", "Mark", "Edward34"],
                   ["4", "Simon", "!!QWasd"]]
    write_table_to_csv(table_query, "sales")
    assert os.path.isfile("csv_files/sales.csv")
