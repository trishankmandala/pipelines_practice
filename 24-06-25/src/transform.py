import pandas as pd

def transform_table(data):
    
    #removing duplicates if any
    data = data.drop_duplicates()

    #check for null values and NAN
    # print(data.isnull().sum())
    # print(data.isna().sum())

    #changing datatype of order_date column to datetime
    data['order_date'] = pd.to_datetime(data['order_date'])

    data = data.sort_values(by='order_amount',ascending=False)

    return data


