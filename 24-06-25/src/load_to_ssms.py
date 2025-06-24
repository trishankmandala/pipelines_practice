import configparser
import urllib.parse
from sqlalchemy import create_engine
import pandas as pd

def load_to_ssms(data):
    
    #read config
    config = configparser.ConfigParser()
    config.read(r'D:\PythonTutorial\config.ini')

    #load credentials
    username = config['SqlDB']['username']
    password = config['SqlDB']['password']
    server = config['SqlDB']['server']
    database = config['SqlDB']['database']
    driver = config['SqlDB']['driver']

    params = urllib.parse.quote_plus(
        f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    )


    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')
    
    send_data = data
    send_data.to_sql(name='data_from_mysql',con=engine,if_exists='replace',index=False)