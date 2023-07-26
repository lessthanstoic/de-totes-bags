import os
from src_csv_conversion.csv_conversion import csv_conversion
import re
from src_csv_conversion import csv_conversion as cc


# Checks to see if the csv_files directory is created
def test_csv_conversion1():
    csv_conversion()

    expected = True
    directory_path = "csv_files"
    result = os.path.exists(directory_path)
    assert result == expected


# Checks to see if number of the files in csv_files matches the number of tables
def test_csv_conversion2():
    csv_conversion()

    directory_path = "csv_files"
    file_list = os.listdir(directory_path)
    num_files = len(file_list)
    result = num_files
    expected = len(cc.table_list)
    assert result == expected