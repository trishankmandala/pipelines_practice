from  src.extract_data import extract_from_mongo
from src.load import load_to_ssms
from src.insert_into_mongo import extract_from_text, load_to_mongoDB
from src.transform import transform_dynamic
import pandas as pd

records = extract_from_text(r'D:\PythonTutorial\Doc_unstructured_1.txt')
load_to_mongoDB(records)
data = extract_from_mongo()

tdata = transform_dynamic(data)
load_to_ssms(tdata)
