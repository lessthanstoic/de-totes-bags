import boto3
from pprint import pprint
import logging
import pandas as  pd
import calendar
import time



def push_data_in_bucket(file_path, file_name):

    try: 

        client = boto3.client("s3")

        client.upload_file(file_path, "ingested-data-vox-indicium", file_name)

        print(f"The file {file_name} was uploaded")

    except Exception as e:
        raise e





def log_changes_to_db(file_path, file_name):

    try:

        file = pd.read_csv(file_path)

        number_of_changes = len(file)

        current_GMT = time.gmtime()
        time_stamp = calendar.timegm(current_GMT)
        

        client = boto3.client('logs')

        log = { 'timestamp': time_stamp * 1000, 'message': f'Number of changes made to {file_name}: {number_of_changes}'}

        client.put_log_events(
        logGroupName='MyLogger',
        logStreamName='test_stream',
        logEvents=[ log
            ]
        )

    except Exception as e:
        print(e, 'errrooooooor')
        pass