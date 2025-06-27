import json
from pymongo import MongoClient

def extract_from_text(file_path:str):
    with open(file_path,'r') as file:
        return json.load(file)

def load_to_mongoDB(data,db_name='Pipelines',collection_name = '26-06-25_Project_Task'):
    client = MongoClient("mongodb://localhost:27017/")
    db = client[db_name]
    collection = db[collection_name]
    collection.delete_many({})
    collection.insert_many(data)

