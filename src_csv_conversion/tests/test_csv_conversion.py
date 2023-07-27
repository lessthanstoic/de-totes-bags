import os
from src_csv_conversion.csv_conversion import csv_conversion
import re
from src_csv_conversion import csv_conversion as cc


def test_checks_if_csv_file_directory_has_been_created():
    csv_conversion()

    expected = True
    directory_path = "csv_files"
    result = os.path.exists(directory_path)
    assert result == expected


def test_checks_if_the_number_of_files_in_the_csv_files_matches_the_number_of_tables():
    csv_conversion()

    directory_path = "csv_files"
    file_list = os.listdir(directory_path)
    num_files = len(file_list)
    result = num_files
    expected = len(cc.table_list)
    assert result == expected