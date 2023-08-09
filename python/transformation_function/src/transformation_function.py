"""
This script defines a Lambda function for performing
a series of data transformation tasks on different data sources,
followed by Parquet conversion and transfer. The primary purpose
is to orchestrate the execution of various data transformation
functions and manage the creation and transfer of Parquet files.
The script imports transformation functions from other modules,
applies them to specific data sources, and generates Parquet files
based on the transformed data.

Usage:
1. Ensure the necessary libraries (boto3, pprint, logging) are available.
2. Import the required data transformation functions from their
respective modules.
3. Define transformation tasks using the imported functions and
appropriate data sources.
4. Execute the 'transformation_function' to perform data transformation,
Parquet conversion,
   and S3 transfer tasks.

Note:
- This script assumes proper configuration and connectivity to Amazon S3.
- Error handling is provided to catch and log exceptions during the
transformation process.
"""
from python.transformation_function.src.dim_address import (
    dim_address_data_frame)
from python.transformation_function.src.dim_counterparty import (
    dim_counterparty_data_frame)
from python.transformation_function.src.dim_currency import (
    dim_currency_data_frame)
from python.transformation_function.src.dim_design_table import (
    dim_design_table_data_frame)
from python.transformation_function.src.fact_sales_order import (
    fact_sales_order_data_frame, create_and_push_parquet)
from python.transformation_function.src.dim_date_transformation import (
    dim_date_transformation)
from python.transformation_function.src.dim_staff import (
    dim_staff_data_frame)

import boto3
from pprint import pprint
import logging

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def transformation_function(event, context):
    """
    This function performs various data transformations and
    creates Parquet files for different dimensions and facts.
    It uses several data frames from different sources to generate
    transformed data and pushes the results to S3 buckets.

    Parameters:
    - event (dict): The event data passed to the Lambda function.
    - context (LambdaContext): The runtime context object.

    Returns:
    None

    Raises:
    Exception: If an error occurs during any of the transformation
    or data push steps.
    """
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
