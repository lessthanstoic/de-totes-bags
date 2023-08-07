from src.dim_address import (
    dim_address_data_frame)
from src.dim_counterparty import (
    dim_counterparty_data_frame)
from src.dim_currency import (
    dim_currency_data_frame)
from src.dim_design_table import (
    dim_design_table_data_frame)
from src.fact_sales_order import (
    fact_sales_order_data_frame, create_and_push_parquet)
from src.dim_date_transformation import (
    dim_date_transformation)
from src.dim_staff import (
    dim_staff_data_frame)

import boto3
from pprint import pprint
import logging

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def transformation_function(event, context):

    try:
        currency_df = dim_currency_data_frame("currency_changes")
        currency_parquet = create_and_push_parquet(currency_df, "dim_currency")
        pprint(currency_parquet)
    except Exception as e:
        logger.error(e)

    try:
        design_df = dim_design_table_data_frame("design_changes")
        design_parquet = create_and_push_parquet(design_df, "dim_design")
        pprint(design_parquet)
    except Exception as e:
        logger.error(e)

    try:
        counterparty_df = dim_counterparty_data_frame(
            "counterparty_changes", "address")
        counterparty_parquet = create_and_push_parquet(
            counterparty_df, "dim_counterparty")
        pprint(counterparty_parquet)
    except Exception as e:
        logger.error(e)

    try:
        location_df = dim_address_data_frame("address_changes")
        location_parquet = create_and_push_parquet(location_df, "dim_location")
        pprint(location_parquet)
    except Exception as e:
        logger.error(e)

    try:
        sales_order_df = fact_sales_order_data_frame("sales_order_changes")
        sales_order_parquet = create_and_push_parquet(
            sales_order_df, "fact_sales_order")
        pprint(sales_order_parquet)
    except Exception as e:
        logger.error(e)

    try:
        date_df = dim_date_transformation(sales_order_df)
        date_parquet = create_and_push_parquet(date_df, "dim_date")
        pprint(date_parquet)
    except Exception as e:
        logger.error(e)

    try:
        staff_df = dim_staff_data_frame("staff_changes", "department")
        staff_parquet = create_and_push_parquet(staff_df, "dim_staff")
        pprint(staff_parquet)
    except Exception as e:
        logger.error(e)

    try:
        s3 = boto3.client('s3')
        s3.put_object(Bucket='processed-data-vox-indicium',
                      Key="event-trigger.txt", Body="load lambda run")
        logger.info("Transformation lambda completed, text file trigger sent")
    except Exception as e:
        logger.error(e)
