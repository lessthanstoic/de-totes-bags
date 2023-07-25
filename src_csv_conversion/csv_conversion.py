import psycopg2
import csv
import re

#from python.secrets_manager.retrieve_secrets import retrieve_secrets

from ..python.secrets_manager.retrieve_secrets import retrieve_secrets



#




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


# SQL query to fetch all records from a table (replace 'your_table_name' with the actual table name)
query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"

# Execute the query
cursor.execute(query)

# Fetch all records
table_list = cursor.fetchall()


# Print the results

#print(table_list)

x = -1

for table_items in table_list:
    x += 1
    


    #print(table_items)

    string_item = str(table_items)

    string_item = re.sub(r"[,()']", "", string_item)
  #  print(string_item)
    
    query3 = f"SELECT * FROM {string_item}"

  

    query3 = re.sub(r"[,()']", "", query3)




    cursor.execute(query3)
    table = cursor.fetchall()



    file_path = f"{string_item}.csv"
   
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(table)

    print(f"CSV file '{file_path}' created successfully.")





    #print(table)









# query2 = "SELECT * FROM currency"

# cursor.execute(query2)

# table_currency = cursor.fetchall()


#print(table_currency)

#print(table_currency)


# for items in table_currency:
#     pass
#    # print(items)


# file_path = "output.csv"

# with open(file_path, mode='w', newline='') as file:
#     writer = csv.writer(file)
#     writer.writerows(table_currency)

#print(f"CSV file '{file_path}' created successfully.")


# Close the cursor and connection
cursor.close()
connection.close()