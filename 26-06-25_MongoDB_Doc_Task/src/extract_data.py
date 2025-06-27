from pymongo import MongoClient
import pandas as pd
def extract_from_mongo(db_name = 'Pipelines',collection_name = 'doc_structure'):
    client = MongoClient("mongodb://localhost:27017/")
    db = client[db_name]
    collection = db[collection_name]
    data =  list(collection.find({}, {"_id": 0}))  
    return pd.DataFrame(data)
