from src.secret_login import retrieve_secret_details
from src.s3_timestamp import get_s3_timestamp
from src.csv_write import write_table_to_csv
from src.push_data_in_bucket import push_data_in_bucket

import psycopg2
import re
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def postgres_data_capture(event, context):
    try:
        param_store = "postgres-datetime.txt"
        try:
            old_datetime = get_s3_timestamp(param_store)
        except Exception as e:
            logger.error(e)
            raise e  # want to stop the program right now

        try:
            db_login_deets = retrieve_secret_details("Totesys-Access")
        except ClientError as e:

            if e.response['Error']['Code'] == 'DecryptionFailure':
                logger.error(
                    "The requested secret can't be decrypted:", e)
            elif e.response['Error']['Code'] == 'InternalServiceError':
                logger.error('An error occurred on service side:', e)
            else:
                logger.error('Database credentials could not be retrieved:', e)
            raise e

        try:
            connection = psycopg2.connect(
                host=db_login_deets['host'],
                port=int(db_login_deets['port']),
                database=db_login_deets['database'],
                user=db_login_deets['username'],
                password=db_login_deets['password']
            )
            logger.info('Connected to Totesys database...')

        except psycopg2.Error as e:

            logger.error('Unable to connect to the database:', e)

            raise e

        # Create a cursor object to execute SQL queries
        cursor = connection.cursor()

        # Get current datetime
        datetime_query = "SELECT TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS.MS');"
        cursor.execute(datetime_query)
        current_datetime = str(cursor.fetchall())
        current_datetime = re.sub(r"[,()']", "", current_datetime)

        # SQL query to fetch all records from a table
        query = '''SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';'''
        # Execute the query
        cursor.execute(query)

        # Fetch all records
        table_list = cursor.fetchall()

        for table_items in table_list:
            table_name = str(table_items)
            # replace "(" or ")" with ""
            if table_name != "('_prisma_migrations',)":
                table_name = re.sub(r"[,()']", "", table_name)

                table_query = f'''SELECT *
                FROM {table_name}
                WHERE created_at
                BETWEEN timestamp '{old_datetime}'
                AND timestamp '{current_datetime}'
                OR last_updated
                BETWEEN timestamp '{old_datetime}'
                AND timestamp '{current_datetime}';'''
                # table_query = re.sub(r"[,()']", "", table_query)

                full_table_query = f'''SELECT *
                FROM {table_name};'''
                # full_table_query = re.sub(r"[,()']", "", full_table_query)
                try:
                    cursor.execute(table_query)
                except Exception as e:
                    logger.critical('Query error:', e)

                table_changes = cursor.fetchall()
                columns = []
                for col in cursor.description:
                    columns.append(col[0])
                table_changes.insert(0, tuple(columns))

                try:
                    write_table_to_csv(table_changes, f'{table_name}_changes')
                except Exception:
                    logger.error(f'Error writing {table_name}_changes.csv')

                try:
                    push_data_in_bucket('/tmp/csv_files/',
                                        f'{table_name}_changes.csv')
                except FileNotFoundError:
                    logger.error(f'File {table_name}_changes.csv not found')
                except ClientError as e:
                    logger.error('Client error:', e)

                try:
                    cursor.execute(full_table_query)
                except Exception as e:
                    logger.critical('Query error:', e)

                full_table = cursor.fetchall()
                columns = []
                for col in cursor.description:
                    columns.append(col[0])
                full_table.insert(0, tuple(columns))

                try:
                    write_table_to_csv(full_table, table_name)
                except Exception:
                    logger.error(f'Error writing {table_name}.csv')

                try:
                    push_data_in_bucket('/tmp/csv_files/', f'{table_name}.csv')
                except FileNotFoundError:
                    logger.error(f'File {table_name}.csv not found')
                except ClientError as e:
                    logger.error('Client error:', e)

        # Close connections
        cursor.close()
        connection.close()

        with open(f'/tmp/csv_files/{param_store}', 'w') as f:
            f.write(current_datetime)

        try:
            push_data_in_bucket("/tmp/csv_files/", param_store)
        except FileNotFoundError:
            logger.error(f'File {param_store} not found...')
        except ClientError as e:
            logger.error('Client error:', e)

    except Exception as e:
        logger.error('Something unexpected went wrong:', e)
        raise e
