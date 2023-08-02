import pandas as pd
from datetime import datetime
"""This module contains a single function - dim_date_transformation.

    This function converts a passed fact_sales_order_dataframe and converts it
    into a third normalized dim_date dataframe

    Standard use: dim_date_transformation(pandas dataframe handler)
"""


def dim_date_transformation(dataframe):
    """This function takes a pandas dataframe as an input,
        extracts dates as strings from four seperate columns,
        filters for unique dates, and then transforms the data into a
        third normalised form

        Arguments: - dataframe (Pandas dataframe with columns named:
                - created_date
                - last_updated_date
                - agreed_payment_date
                - agreed_delivery_date
                with string values)

        Returns: - dataframe (Pandas dataframe with columns named:
                date_id (string)
                year (int)
                month (int)
                day (int)
                day_of_week (int)
                day_name (string)
                month_name (string)
                quarter (int))

        Errors: - KeyError - if dataframe does not contain necessary columns
                - ValueError - if date columns are correctly formatted
                - TypeError - if not passed dataframe as argument"""

    try:

        created_list = list(dataframe['created_date'])
        updated_list = list(dataframe['last_updated_date'])
        payment_list = list(dataframe['agreed_payment_date'])
        delivery_list = list(dataframe['agreed_delivery_date'])

        all_dates = created_list + updated_list + payment_list + delivery_list
        unique_dates = []
        [unique_dates.append(x) for x in all_dates if x not in unique_dates]

        print(unique_dates)

        output_data = {'date_id': unique_dates, 'year': [],
                       'month': [], 'day': [], 'day_of_week': [],
                       'day_name': [], 'month_name': [], 'quarter': []}

        for date in unique_dates:

            date_format = '%d/%m/%Y'

            date_obj = datetime.strptime(date, date_format)

            print(date_obj)

            output_data['year'].append(date_obj.year)
            output_data['month'].append(date_obj.month)
            output_data['day'].append(date_obj.day)
            output_data['day_of_week'].append(date_obj.weekday())
            output_data['day_name'].append(date_obj.strftime('%A'))
            output_data['month_name'].append(date_obj.strftime('%B'))
            output_data['quarter'].append((date_obj.month-1)//3 + 1)

        return pd.DataFrame(data=output_data)

    except KeyError as e:
        print('Error while producing dim_date: column does not exist')
        print(e)
        raise e

    except ValueError as e:
        print('Error while producing dim_date: column not in expected format')
        raise e

    except TypeError as e:
        print('Error while producing dim_date: argument not a dataframe')
        raise e
