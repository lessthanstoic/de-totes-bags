"""
This module contains a function for writing data to CSV files.

The primary purpose of this module is to provide a convenient way
to write data in tabular form
to CSV files. The provided function, `write_table_to_csv`, takes a
table of data in the form of
a list of lists and writes it to a CSV file located in a specified directory.

Usage:
1. Ensure the necessary libraries (os, csv) are available.
2. Utilize the `write_table_to_csv` function to create a CSV file
with provided data.
   - Pass a table of data (list of lists) and a tablename as arguments.
   - The function will create a CSV file named 'tablename.csv' in the
   '/tmp/csv_files' directory.
3. The created CSV file will contain the data from the provided table.

Example:
Assuming a data table is defined as:
data_table = [
    ["Name", "Age", "Location"],
    ["Alice", 25, "New York"],
    ["Bob", 30, "San Francisco"],
    ["Carol", 22, "Los Angeles"]
]

write_table_to_csv(data_table, "people_info")

This will create a CSV file named 'people_info.csv' in
the '/tmp/csv_files' directory.
"""

import os
import csv


def write_table_to_csv(table, tablename):
    """
    Write a table of data to a CSV file.

    Args:
        table (list of lists): The table of data to be written.
        tablename (str): The name of the CSV file (without extension).

    Returns:
        None
    """
    csv_directory = "/tmp/csv_files"
    if not os.path.exists(csv_directory):
        os.makedirs(csv_directory)

    file_path = os.path.join(csv_directory, f"{tablename}.csv")

    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(table)
