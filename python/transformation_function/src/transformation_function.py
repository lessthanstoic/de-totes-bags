from python.transformation_function.src.dim_address import dim_address_data_frame
from python.transformation_function.src.dim_counterparty import dim_counterparty_data_frame
from python.transformation_function.src.dim_currency import dim_currency_data_frame
from python.transformation_function.src.dim_date_transformation import dim_date_transformation
from python.transformation_function.src.dim_design_table import design_table_data_frame
from python.transformation_function.src.fact_sales_order import fact_sales_order_data_frame, create_and_push_parquet

from pprint import pprint

def transformation_function(event, context):
    
    currency_df = dim_currency_data_frame("currency_changes")
    currency_parquet = create_and_push_parquet(currency_df,"currency_changes")
    pprint(currency_parquet)

    design_df = design_table_data_frame("design_changes")
    design_parquet = create_and_push_parquet(design_df,"design_changes")
    pprint(design_parquet)

    counterparty_df = dim_counterparty_data_frame("counterparty_changes", "address")
    counterparty_parquet = create_and_push_parquet(counterparty_df, "counterparty_changes")
    pprint(counterparty_parquet)
    
