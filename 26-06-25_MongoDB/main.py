from src.insertIntoMongoDB import extract_from_text,load_to_mongoDB
from src.extractData import read_data
from src.load import load_to_ssms
records = extract_from_text(r'D:\PythonTutorial\26-06-25_MongoDB\project.txt')
data = load_to_mongoDB(records)
fetched_data = read_data()

# print(fetched_data['technologies'])
# print(fetched_data.dtypes)


load_to_ssms(fetched_data)
