import configparser
import urllib.parse
from sqlalchemy import create_engine
import pandas as pd
import urllib

def load_unstructured_to_ssms(data,table_name):
    config = configparser.ConfigParser()
    config.read(r'D:\PythonTutorial\config.ini')

    username = config['Sql']['username']
    password = config['Sql']['password']
    server = config['Sql']['server']
    driver = config['Sql']['driver']
    database = config['Sql']['database']

    params = urllib.parse.quote_plus(
        f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    )

    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

    data.to_sql(name=table_name,con=engine,if_exists='replace',index=False)
    



