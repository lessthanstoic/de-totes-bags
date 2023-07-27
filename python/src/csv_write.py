import os
import csv


def write_table_to_csv(table, tablename):

    csv_directory = "csv_files"
    if not os.path.exists(csv_directory):
        os.makedirs(csv_directory)

    file_path = os.path.join(csv_directory, f"{tablename}.csv")

    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(table)