import pandas as pd

"""
argv should be the dimension tables
All arguments should be pandas dataframes
"""
def CreateStarSchema(factTable, *argv):
    df_starschema = factTable
    for arg in argv:
        df_starschema = pd.merge(factTable, arg, left_on="", right_on="")
    pass


# factTable = pd.DataFrame(
#     {'created_date': [],
#     'last_updated_date': [],
#      'agreed_payment_date': [],
#       'agreed_delivery_date': [],
#        'sales_staff_id': [] })

# date_df = pd.DataFrame(
#     { 'date_id': ["a", "b", "c", "d", "e"],
#       'quarter': [1,1,4,2,3] })

# staff_df = pd.DataFrame(
#     { 'staff_id': [123, 321, 213, 312, 454],
#       'first_name': ["Sue", "Sue", "Sue", "Sue", "Sue"] })

# factTable['created_date'] = date_df['date_id']
# factTable['last_updated_date'] = date_df['date_id']
# factTable['agreed_payment_date'] = date_df['date_id']
# factTable['agreed_delivery_date'] = date_df['date_id']

# factTable = pd.merge( factTable, staff_df, left_on="sales_staff_id", right_on="staff_id", how="outer" )

csvpath = "/home/activehelix/Documents/Coding/DataEngineering/Northcoders/Project/csvs/sales_order.csv"
factTable = pd.read_csv(csvpath, header=None)
column_names = ["sales_order_id","created_date","created_time","sales_staff_id","counterparty_id","units_sold","unit_price","currency_id","design_id","agreed_payment_date","agreed_delivery_date","agreed_delivery_location_id"]
factTable.columns = column_names

print(factTable)






# m = pd.merge(factTable, date_df, how="outer", left_on="created_date", right_on="date_id")
# merged_df = pd.concat([factTable, date_df], ignore_index=True)

# print(m)
# print(merged_df)