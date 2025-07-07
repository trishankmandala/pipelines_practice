import pandas as pd

def transform_table(data):
    
    #removing duplicates if any
    data = data.drop_duplicates()
    return data


