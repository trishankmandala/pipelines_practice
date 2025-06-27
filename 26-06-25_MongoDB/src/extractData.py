from pymongo import MongoClient
import pandas as pd
def read_data(db_name = 'Pipelines',collection_name = '26-06-25_Project_Task'):
    client = MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    collection = db[collection_name]
    data = list(collection.find({},{'_id':0}))
    df =  pd.DataFrame(data)
    for col in df.columns:
        df[col] = df[col].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
    return df 
