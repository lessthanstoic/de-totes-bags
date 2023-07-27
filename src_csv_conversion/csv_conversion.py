import psycopg2
import csv
import re
import os


def csv_conversion():
    global table_list

    secrets = []
    file_path = 'secrets.txt'
    regex_host = ":"
    regex_host_end = ","
    input_labels = ["User", "Host", "Port", "Database", "Password"]
    inputs = {}

    with open(file_path, 'r') as file:
        # Should be changed as len(the file's number of lines)
        for i in range(5):
            line = file.readline()
            secrets.append(line)

    for label, secret in zip(input_labels, secrets):
        input_value = ""
        start_inputting = False
        for char in secret:
            if start_inputting:
                input_value += char
            if char == regex_host:
                start_inputting = True
            if char == regex_host_end:
                input_value = input_value[:-1]
                start_inputting = False
                break
        inputs[label] = input_value

    host = inputs["Host"]
    port = inputs["Port"]
    database = inputs["Database"]
    user = inputs["User"]
    password = inputs["Password"]

    # Connect to the database
    try:
        connection = psycopg2.connect(
            host=host, port=port, database=database, user=user, password=password
        )
        print("Connected to the database!")
    except psycopg2.Error as e:
        print(f"Error: Unable to connect to the database - {e}")

    # Create a cursor object to execute SQL queries
    cursor = connection.cursor()

    # SQL query to fetch all records from a table
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"

    # Execute the query
    cursor.execute(query)

    # Fetch all records
    table_list = cursor.fetchall()

    # 71 to 73 in another function
    csv_directory = "csv_files"
    if not os.path.exists(csv_directory):
        os.makedirs(csv_directory)

    for table_items in table_list:
       

        string_item = str(table_items)
        string_item = re.sub(r"[,()']", "", string_item)

        query = f"SELECT * FROM {string_item}"
        query = re.sub(r"[,()']", "", query)

        cursor.execute(query)
        table = cursor.fetchall()

        # 90 to 96 in another function
        file_path = os.path.join(csv_directory, f"{string_item}.csv")

        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(table)

        print(f"CSV file '{file_path}' created successfully.")

    cursor.close()
    connection.close()
    # return table


