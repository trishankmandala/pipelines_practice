from pymongo import MongoClient

def read_data(db_name = 'Pipelines',collection_name = '26-06-25_Project_Task'):
    client = MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    collection = db[collection_name]
    data_from_mongo = list(collection.find({},{'_id':0}))
    return data_from_mongo
