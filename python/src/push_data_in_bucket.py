import boto3
from pprint import pprint
import logging
import pandas as  pd

def push_data_in_bucket(file_path, file_name):

    try: 


        client = boto3.client("s3")

        client.upload_file(file_path, "ingested-data-vox-indicium", file_name)

        print(f"The file {file_name} was uploaded")

    except Exception as e:
        raise e
    



logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

def log_changes_to_db(file_path, file_name):

    # How many changes have been made == How many lines there are in the csv
    """Steps:
     - Read .csv file
     - Count number of rows
     - log number of rows to Cloudwatch"""

    try:

        file = pd.read_csv(file_path)
        pprint(file)
        number_of_changes = len(file)
        print(number_of_changes)

        logger.info(f'Number of changes made to {file_name}: {number_of_changes}')
        return number_of_changes

    except Exception as e:
        print(e)
    pass