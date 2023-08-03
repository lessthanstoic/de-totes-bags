from python.transformation_function.src.dim_address import dim_address_data_frame
from python.transformation_function.src.dim_counterparty import dim_counterparty_data_frame
from python.transformation_function.src.dim_currency import dim_currency_data_frame
from python.transformation_function.src.dim_date_transformation import dim_date_transformation
from python.transformation_function.src.dim_design_table import design_table_data_frame
from python.transformation_function.src.fact_sales_order import fact_sales_order_data_frame

from pprint import pprint

def transformation_function(event, context):
    
    currency_df = dim_currency_data_frame("currency")
    pprint(currency_df)

