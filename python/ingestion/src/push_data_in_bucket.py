import boto3
import pandas as pd
import calendar
import time


def push_data_in_bucket(file_path, file_name):

    try:
        log_changes_to_db(file_path, file_name)
        client = boto3.client("s3")

        client.upload_file(file_path, "ingested-data-vox-indicium", file_name)

        print(f"The file {file_name} was uploaded")

    except FileNotFoundError as e:
        print(f'File {file_path} not found')
        raise e


def log_changes_to_db(file_path, file_name):

    try:

        file = pd.read_csv(file_path)

        num = len(file)

        current_GMT = time.gmtime()
        time_stamp = calendar.timegm(current_GMT)

        client = boto3.client('logs')

        log = {'timestamp': time_stamp * 1000,
               'message': f'Number of changes made to {file_name}: {num}'}

        client.put_log_events(
            logGroupName='MyLogger',
            logStreamName='test_stream',
            logEvents=[log
                       ]
        )

    except Exception as e:
        print(e, 'errrooooooor')
        pass