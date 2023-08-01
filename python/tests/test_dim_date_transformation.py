from python.src_dim_date.dim_date_transformation import dim_date_transformation
import pandas as pd


d = {'sales_record_id': [1, 2], 'sales_order_id': [13, 54], 'created_date': ['12/07/2023', '24/07/2023'], 'created_time': ['13:00', '14:30'], 'last_updated_date': ['16/07/2023', '18/07/2023'], 'last_updated_time': ['12:00', '14:00'], 'sales_staff_id': [4, 7], 'counterparty_id': [ 34, 2
], 'units_sold': [4, 5], 'unit_price': [ 6.54, 4.22 ], 'currency_id': [1, 3], 'design_id': [1, 1], 'agreed_payment_date': ['31/07/2023', '02/08/2023'], 'agreed_delivery_date': ['31/07/2023', '02/08/2023'], 'agreed_delivery_location_id': [3, 7]}

mock_df = pd.DataFrame(data=d)

    # mess about with them - divide into year, month, day_of_week, day_of_week, month_name, quarter:

xd = {'date_id' : ['12/07/2023', '24/07/2023', '16/07/2023', '18/07/2023', '31/07/2023', '02/08/2023'], 'year'  : ['2023', '2023', '2023', '2023', '2023', '2023' ], 'month' : [7, 7, 7, 7, 7, 7, 8], 'day_of_week' : [], 'month_name' : [], 'quarter': []}
# expected_df = 

def test_dim_date_transformation():

    dim_date_transformation(mock_df)

    assert False
    pass