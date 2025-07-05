import configparser
import urllib.parse
from sqlalchemy import create_engine
import pandas as pd

def load_to_ssms(data):
    config = configparser.ConfigParser()
    config.read(r'D:\PythonTutorial\config.ini')

    username = config['Sql']['username']
    password = config['Sql']['password']
    server   = config['Sql']['server']
    database = config['Sql']['database']
    driver   = config['Sql']['driver']

    params = urllib.parse.quote_plus(
        f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    )

    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

    data.to_sql(name="sentiments_of_tweets",con=engine,if_exists='replace',index=False)