import psycopg2
import csv
import re

secrets = [] 
file_path = '../de-totes-bags/secrets.txt'
try:
    with open(file_path, 'r') as file:
        # Step 2: Read the file using read() method
        # file_contents = file.read()
        # You can also use readline() to read a single line at a time
        for i in range(4):
            line = file.readline()
            secrets.append(line)
        print(secrets[0])


except FileNotFoundError:
    print(f"File not found: '{file_path}'")

# The file will be automatically closed after the 'with' block ends.



host = "nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com"
port = 5432
database = "totesys"
user = "project_user_1"
password = "wgvk8E1JvSdZcMvbvXda3dna"


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


for table_items in table_list:

    string_item = str(table_items)

    string_item = re.sub(r"[,()']", "", string_item)

    
    query3 = f"SELECT * FROM {string_item}"
    query3 = re.sub(r"[,()']", "", query3)

    cursor.execute(query3)
    table = cursor.fetchall()

    file_path = f"{string_item}.csv"
   
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(table)

    print(f"CSV file '{file_path}' created successfully.")


cursor.close()
connection.close()