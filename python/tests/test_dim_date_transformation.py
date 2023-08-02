from python.src_dim_date.dim_date_transformation import dim_date_transformation
import pandas as pd
from pytest import raises


def test_extracts_dates_from_four_columns_fact_table_and_transforms_data():

    d = {'sales_record_id': [1, 2], 'sales_order_id': [13, 54],
         'created_date': ['12/01/2023', '24/12/2023'],
         'created_time': ['13:00', '14:30'],
         'last_updated_date': ['16/04/2023', '18/07/2023'],
         'last_updated_time': ['12:00', '14:00'],
         'sales_staff_id': [4, 7], 'counterparty_id': [34, 2],
         'units_sold': [4, 5], 'unit_price': [6.54, 4.22],
         'currency_id': [1, 3], 'design_id': [1, 1],
         'agreed_payment_date': ['31/07/2023', '02/08/2023'],
         'agreed_delivery_date': ['31/07/2023', '02/08/2023'],
         'agreed_delivery_location_id': [3, 7]}

    mock_df = pd.DataFrame(data=d)

    xd = {'date_id': ['12/01/2023', '24/12/2023', '16/04/2023',
                      '18/07/2023', '31/07/2023', '02/08/2023'],
          'year': [2023, 2023, 2023, 2023, 2023, 2023],
          'month': [1, 12, 4, 7, 7, 8], 'day': [12, 24, 16, 18, 31, 2],
          'day_of_week': [3, 6, 6, 1, 0, 2],
          'day_name': ['Thursday', 'Sunday', 'Sunday',
                       'Tuesday', 'Monday', 'Wednesday'],
          'month_name': ['January', 'December',
                         'April', 'July', 'July', 'August'],
          'quarter': [1, 4, 2, 3, 3, 3]}

    expected_df = pd.DataFrame(data=xd)

    result = dim_date_transformation(mock_df)

    pd.testing.assert_frame_equal(result, expected_df)


def test_function_raises_error_when_date_columns_missing():

    d = {'sales_record_id': [1, 2], 'sales_order_id': [13, 54],
         'created_date': ['12/01/2023', '24/12/2023'],
         'last_updated_time': ['12:00', '14:00'], 'sales_staff_id': [4, 7],
         'counterparty_id': [34, 2],
         'units_sold': [4, 5], 'unit_price': [6.54, 4.22],
         'currency_id': [1, 3], 'design_id': [1, 1],
         'agreed_payment_date': ['31/07/2023', '02/08/2023'],
         'agreed_delivery_date': ['31/07/2023', '02/08/2023'],
         'agreed_delivery_location_id': [3, 7]}

    mock_df = pd.DataFrame(data=d)

    with raises(KeyError):

        dim_date_transformation(mock_df)


def test_function_raises_error_when_dates_are_incorrectly_formatted():

    d = {'sales_record_id': [1, 2], 'sales_order_id': [13, 54],
         'created_date': ['12/01/2023', '24/12/2023'],
         'created_time': ['13:00', '14:30'],
         'last_updated_date': ['2023-09-03', '2023-19-02'],
         'last_updated_time': ['12:00', '14:00'],
         'sales_staff_id': [4, 7], 'counterparty_id': [34, 2],
         'units_sold': [4, 5], 'unit_price': [6.54, 4.22],
         'currency_id': [1, 3], 'design_id': [1, 1],
         'agreed_payment_date': ['31/07/2023', '02/08/2023'],
         'agreed_delivery_date': ['31/07/2023', '02/08/2023'],
         'agreed_delivery_location_id': [3, 7]}

    mock_df = pd.DataFrame(data=d)

    with raises(ValueError):

        dim_date_transformation(mock_df)

    pass

def test_raises_error_if_not_passed_dataframe():

    with raises(TypeError):
        dim_date_transformation([2, 4, 5, 6, 7])

    pass