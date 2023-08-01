import pandas as pd
from datetime import datetime

def dim_date_transformation(dataframe):

    print(dataframe)

    print(dataframe.values)

    created_list = list(dataframe['created_date'])
    updated_list = list(dataframe['last_updated_date'])
    payment_list = list(dataframe['agreed_payment_date'])
    delivery_list = list(dataframe['agreed_delivery_date'])

    all_dates = created_list + updated_list + payment_list + delivery_list
    unique_dates = []
    [unique_dates.append(x) for x in all_dates if x not in unique_dates]

    print(unique_dates)

    output_data = {'date_id' : unique_dates, 'year'  : [], 'month' : [], 'day' : [], 'day_of_week' : [], 'day_name' : [], 'month_name' : [], 'quarter': []}


    for date in unique_dates:

        date_format = '%Y/%m/%d'

        date_obj = datetime.strptime(date, date_format)
        
        print(date_obj)
        # output_data['year'].append(    )
        # output_data['month'].append(    )
        # output_data['day'].append(    )
        # output_data['day_of_week'].append(    )
        # output_data['day_name'].append(    )
        # output_data['month_name'].append(    )
        # output_data['quarter'].append(    )

        pass



    # bring in dataframe of fact_sales_order_table ✅
    # look at 4 different columns (ending in date) ✅
    # filter unique dates - is there a way to do this in pandas? (populate list with dates and remove duplicates) ✅
    # mess about with them - divide into year, month, day_of_week, day_of_week, month_name, quarter:
    # again, can we do this is pandas? if not python has a package
    # repopulate dataframe with this information
    # export new dataframe



    pass